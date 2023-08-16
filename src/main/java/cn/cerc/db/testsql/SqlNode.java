package cn.cerc.db.testsql;

import java.util.List;

public class SqlNode {
    // 语句
    private String sql;

    // 右侧的连词(and, or)
    private String conj;

    // 子节点
    private List<SqlNode> subNodes;

    // 判断是否有子语句
    public boolean hasSubNodes() {
        return subNodes != null && subNodes.size() != 0;
    }

    // 添加子节点
    public SqlNode addSubNode(SqlNode sub) {
        this.subNodes.add(sub);
        return this;
    }

    public SqlNode addSubNodeList(List<SqlNode> list) {
        this.subNodes.addAll(list);
        return this;
    }

    // 设置语句
    public SqlNode setSql(String sql) {
        this.sql = sql;
        return this;
    }

    // 设置连词
    public SqlNode setConj(String conj) {
        this.conj = conj;
        return this;
    }

    public String getSql() {
        return this.sql;
    }

    public String getConj() {
        return this.getConj();
    }

    public List<SqlNode> getSubNodes() {
        return this.subNodes;
    }

}
