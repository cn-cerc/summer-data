package cn.cerc.db.core;

import java.util.Arrays;

public class FileUtils {

    public static final String[] IMAGE_SUFFIX = new String[] { "jpg", "png", "jpeg", "gif", "bmp" };

    /**
     * 校验文件名称是否以 suffix 后缀结尾
     * 
     * @param fileName 文件名称
     * @param suffix   后缀
     * @return
     */
    public static boolean verifySuffix(String fileName, String... suffix) {
        if (Utils.isEmpty(fileName))
            return false;
        if (suffix == null || suffix.length == 0)
            return false;
        return Arrays.stream(suffix).anyMatch(fileName::endsWith);
    }

    /**
     * 校验文件名称是否是图片
     * 
     * @param fileName 文件名称
     * @return
     */
    public static boolean isImage(String fileName) {
        return verifySuffix(fileName, IMAGE_SUFFIX);
    }

    /**
     * 重命名文件名
     * 
     * @param fileName 需要带有后缀的文件名称 dog.jpg
     * @param newName  重命名的文件名称 cat
     * @return 新名称 cat.jpg
     */
    public static String rename(String fileName, String newName) {
        if (fileName == null)
            throw new NullPointerException("文件名称不能为空");
        if (newName == null)
            throw new NullPointerException("重命名文件名称不能为空");
        int index = fileName.lastIndexOf('.');
        if (index < 0)
            throw new RuntimeException("文件名称不存在后缀");
        return newName.concat(fileName.substring(index));
    }

    public static void main(String[] args) {
        String fileName = "dog.jpg";
        System.out.println(verifySuffix(fileName, "gif"));
        System.out.println(isImage(fileName));
        System.out.println(rename(fileName, Utils.getStrRandom(10)));
        System.out.println(rename("dog", Utils.getStrRandom(10)));
    }

}
