package cn.cerc.db.mongo;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.apache.commons.io.FilenameUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;

import cn.cerc.db.core.Utils;

/**
 * 使用MongoDB的 GridFS Bucket 存储二进制文件
 */
public class MongoOSS {
    private static final Logger log = LoggerFactory.getLogger(MongoOSS.class);
    private static final String BucketName = "moss";
    private static volatile GridFSBucket bucket;

    public static GridFSBucket bucket() {
        if (bucket == null)
            synchronized (MongoOSS.class) {
                if (bucket == null) {
                    var mgdb = MongoConfig.getDatabase();
                    bucket = GridFSBuckets.create(mgdb, BucketName);
                }
            }
        return bucket;
    }

    public static void writeFile(String filename) {
        try (InputStream streamToUploadFrom = new FileInputStream(filename)) {
            upload(filename, streamToUploadFrom, null);
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public static Optional<String> upload(String url) {
        var readStream = getWebfile(url);
        if (readStream.isPresent()) {
            Optional<String> childUrl = getChildUrl(url);
            if (childUrl.isPresent())
                url = childUrl.get();
            url = Utils.decode(url, StandardCharsets.UTF_8);
            return Optional.of(upload(url, readStream.get(), null));
        } else
            return Optional.empty();
    }

    /**
     * 迁移使用
     */
    public static String upload(String url, InputStream fileStream, Consumer<Document> onsumer) {
        Objects.requireNonNull(url);
        String filename = url;
        if (!url.startsWith("/"))
            filename = "/" + url;
        var find = MongoOSS.findByName(filename);
        if (find.isEmpty()) {
            var doc = new Document();
            if (onsumer != null)
                onsumer.accept(doc);
            if (!doc.containsKey("filename"))
                doc.append("filename", filename);
            var options = new GridFSUploadOptions().chunkSizeBytes(1048576).metadata(doc);
            ObjectId fileId = bucket().uploadFromStream(filename, fileStream, options);
            var result = fileId.toHexString();
            log.info("append file id: {}", result);
            return result;
        } else {
            var result = find.get().getObjectId().toHexString();
            log.info("exists file id: {}", result);
            return result;
        }
    }

    /**
     * 接收用户上传使用
     */
    public static int receive(HttpServletRequest request, List<String> fields, Consumer<String> output) {
        int total = 0;
        try {
            DiskFileItemFactory factory = new DiskFileItemFactory();
            factory.setSizeThreshold(5120);
            ServletFileUpload upload = new ServletFileUpload(factory);
            List<FileItem> uploadFiles = upload.parseRequest(request);
            if (uploadFiles.size() > 0) {
                // 先读取参数
                HashMap<String, String> names = new HashMap<>();
                for (FileItem fileItem : uploadFiles) {
                    if (fileItem.isFormField()) {
                        if (fields.contains(fileItem.getFieldName())) {
                            var value = new String(fileItem.getString().getBytes(StandardCharsets.ISO_8859_1),
                                    StandardCharsets.UTF_8);
                            names.put(fileItem.getFieldName(), value);
                        }
                    }
                }
                // 再读取文件
                for (FileItem fileItem : uploadFiles) {
                    if (!fileItem.isFormField()) {
                        if (fileItem.getSize() != 0L) {
                            var filename = fileItem.getName().toLowerCase();
                            var suffix = "." + FilenameUtils.getExtension(filename);
                            try {
                                Consumer<Document> onsumer = doc -> {
                                    doc.append("suffix", suffix);
                                    names.forEach(doc::append);
                                };
                                var url = "/temp/" + filename;
                                if (MongoOSS.findByName(url).isEmpty()) {
                                    MongoOSS.upload(url, fileItem.getInputStream(), onsumer);
                                    output.accept("upload file: " + filename);
                                    total++;
                                } else {
                                    output.accept("file exists: " + filename);
                                }
                            } catch (IOException e) {
                                log.error(e.getMessage(), e);
                            }
                        } else {
                            // 文件大小为0
                        }
                    }
                }
            }
        } catch (Exception e) {
            output.accept(e.getMessage());
            return 0;
        }
        return total;
    }

    /**
     * @param hossFileId hoss的存储文件id
     * @return 返回 gridFs 文件对象
     */
    public static Optional<GridFSFile> findById(String hossFileId) {
        var objectId = new ObjectId(hossFileId);
        var result = MongoOSS.bucket().find(new BasicDBObject("_id", objectId)).first();
        return Optional.ofNullable(result);
    }

    /**
     * @param filename hoss的存储文件名称
     * @return 返回 gridfs 文件对象
     */
    public static Optional<GridFSFile> findByName(String filename) {
        if (!filename.startsWith("/"))
            filename = "/" + filename;
        var result = MongoOSS.bucket().find(new BasicDBObject("filename", filename)).first();
        return Optional.ofNullable(result);
    }

    /**
     * @param filename hoss的存储文件名称
     * @return 返回 filename 文件对象是否在mongodb文件库里存在
     */
    public static boolean exist(String filename) {
        Optional<GridFSFile> fsFile = findByName(filename);
        return fsFile.isPresent();
    }

    /**
     * @param fileNameSource hoss的存储文件源文件名称
     * @param fileNameTarget hoss的存储文件目标文件名称
     * @return 返回 filename 文件对象是否复制成功
     */
    public static boolean copy(String fileNameSource, String fileNameTarget) {
        Optional<GridFSFile> fsFile = findByName(fileNameSource);
        if (fsFile.isEmpty())
            return false;

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        bucket().downloadToStream(fsFile.get().getObjectId(), outputStream);

        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        upload(fileNameTarget, inputStream, null);
        return true;
    }

    /**
     * 下载mongodb文件为输入流
     */
    public static InputStream download(String fileName) {
        if (!fileName.startsWith("/"))
            fileName = "/" + fileName;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        MongoOSS.bucket().downloadToStream(fileName, outputStream);
        return new ByteArrayInputStream(outputStream.toByteArray());
    }

    /**
     * 下载web文件到指定的本地文件
     */
    public void download(String webfileUrl, String localFilename) throws IOException {
        var readStream = getWebfile(webfileUrl);
        if (readStream.isEmpty())
            return;
        // 创建文件
        File file = new File(localFilename);
        if (file.exists()) {
            log.warn("{} 文件被覆盖", localFilename);
            transmitStream(readStream.get(), new FileOutputStream(localFilename));
        } else {
            file.createNewFile();
            transmitStream(readStream.get(), new FileOutputStream(localFilename));
            log.warn("{} 文件下载完成", localFilename);
        }
    }

    /**
     * 传送数据流
     */
    private void transmitStream(InputStream readStream, FileOutputStream writeStream) throws IOException {
        try (BufferedInputStream out = new BufferedInputStream(readStream);
             BufferedOutputStream bos = new BufferedOutputStream(writeStream)) {
            int bytes;
            byte[] bufferOut = new byte[1024];
            while ((bytes = out.read(bufferOut)) != -1) {
                bos.write(bufferOut, 0, bytes);
            }
        }
    }

    /**
     * 取得网络文件流
     */
    public static Optional<InputStream> getWebfile(String webfileUrl) {
        try {
            URL url = new URL(webfileUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod(connection.getRequestMethod());
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setRequestProperty("User-Agent", "Mozilla/4.76");
            connection.setRequestProperty("connection", "keep-alive");
            connection.setDoOutput(true);
            return Optional.ofNullable(connection.getInputStream());
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    /**
     * 从Mongo中删除OSS文件
     *
     * @param fileName 文件名
     */
    public static void delete(String fileName) {
        if (fileName.startsWith("http")) {
            Optional<String> childUrl = getChildUrl(fileName);
            if (childUrl.isPresent())
                fileName = childUrl.get();
        }
        if (!fileName.startsWith("/"))
            fileName = "/" + fileName;
        Optional<GridFSFile> result = findByName(fileName);
        if (result.isPresent()) {
            var objectId = result.get().getObjectId();
            bucket().delete(objectId);
        }
    }

    /**
     * @param url http(s)://4plc.oss-cn-hangzhou.aliyuncs.com/abc.jpg
     * @return /abc.jpg
     */
    public static Optional<String> getChildUrl(String url) {
        if (!url.startsWith("http"))
            return Optional.empty();
        var start = url.indexOf("//");
        var str = url.substring(start + 2);
        var point = str.indexOf("/");
        var result = str.substring(point);
        return Optional.of(result);
    }

    /**
     * @return 列出所有的文件
     */
    public static ArrayList<GridFSFile> list() {
        return MongoOSS.bucket().find().into(new ArrayList<>());
    }

}
