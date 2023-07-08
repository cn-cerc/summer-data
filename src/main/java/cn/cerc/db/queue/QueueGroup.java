package cn.cerc.db.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class QueueGroup {
    private String code;
    private List<AbstractQueue> items = new ArrayList<>();
    private int executionSequence = 0;

    public QueueGroup(String code) {
        this.code = code;
    }

    /**
     * 消息分组代码
     */
    public String code() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    /**
     * 开始登记下一批
     * 
     * @return
     */
    public <T extends AbstractQueue> T addBatch(T queue) {
        Objects.requireNonNull(this.code);

        // 处理上一批
        if (items.size() == 0)
            throw new RuntimeException("当前批为空，不允许登记下一批");
        for (var item : items)
            item.saveToSqlmg();

        items.clear();
        // 增加批次号
        this.executionSequence++;

        // 注册新的队列
        queue.setGroupCode(this.code);
        queue.setExecutionSequence(this.executionSequence);
        items.add(queue);
        return queue;
    }

    /**
     * 
     * @return 执行次序号
     */
    public int getExecutionSequence() {
        return executionSequence;
    }
}
