package cn.cerc.db.queue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.aliyun.mns.model.Message;

public abstract class AbstractTopicQueue extends AbstractDataRowQueue {

    public AbstractTopicQueue() throws Exception {
        super();
    }

    @Override
    protected String getMessageBody(Message msg) {
        var items = decode(msg.getMessageBodyAsRawString());
        return items.get("Message");
    }

    private static Map<String, String> decode(String xmlData) {
        Map<String, String> items = new HashMap<>();
        DocumentBuilderFactory factory = DocumentBuilderFactory.newDefaultInstance();
        try {
            ByteArrayInputStream text = new ByteArrayInputStream(xmlData.getBytes(Charset.forName("utf-8")));
            Document xml = factory.newDocumentBuilder().parse(text);
            NodeList list = xml.getDocumentElement().getChildNodes();
            for (int i = 0; i < list.getLength(); i++) {
                var node = list.item(i);
                if ("#text".equals(node.getNodeName()))
                    continue;
                items.put(node.getNodeName(), node.getTextContent());
            }
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }
        return items;
    }

}
