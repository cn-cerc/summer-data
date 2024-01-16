package cn.cerc.db.elasticsearch;

import java.util.Arrays;

import javax.persistence.Id;

import cn.cerc.db.core.Datetime;
import cn.cerc.db.elasticsearch.annotation.Document;
import cn.cerc.db.elasticsearch.annotation.Field;
import cn.cerc.db.elasticsearch.enums.FieldType;

@Document(indexName = ElasticsearchEntity.INDEXNAME)
public class ElasticsearchEntity {

    public static final String INDEXNAME = "my_index";

    @Id
    private Long id;

    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    private String name;

    @Field(type = FieldType.Keyword)
    private String sex;

    @Field(type = FieldType.Integer)
    private Integer age;

    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    private String remark;

    @Field(type = FieldType.Keyword)
    private String[] tag;

    @Field(type = FieldType.Text, analyzer = "ik_max_word")
    private String addressLocation;

    @Field(type = FieldType.Keyword)
    private String birthAddress;

    @Field(type = FieldType.Date, format = "yyyy-MM-dd HH:mm:ss")
    private Datetime createTime;

    @Field(type = FieldType.Boolean)
    private Boolean hasGirlFriend;

    // 下面都是为了生成测试数据而准备的
    private final static String[] city = new String[] { "深圳", "广州", "上海", "北京", "武汉" };
    private final static String[] address = new String[] { "北京市朝阳区北辰东路15号", "上海市黄浦区人民大道200号", "深圳市福田区福中三路市民中心C区",
            "武汉市江岸区一元街道沿江大道188号", "广州市花都区新华街新都大道68号" };

    public static ElasticsearchEntity getRandomData(long id) {
        ElasticsearchEntity ElasticsearchEntity = new ElasticsearchEntity();
        ElasticsearchEntity.setId(id);
        ElasticsearchEntity.setName(RandomUtil.randomString("张三李四王五陈六江文档词测试", 3));
        ElasticsearchEntity.setSex(id % 2 == 0 ? "男" : "女");
        ElasticsearchEntity.setAge(RandomUtil.randomInt(15, 30));
        ElasticsearchEntity.setRemark(
                RandomUtil.randomString("活波开朗，具有进取精神和团队精神，有较强的动手能力。良好协调沟通能力，适应力强，反应快、积极、细心、灵活， 具有一定的社会交往能力", 15));
        ElasticsearchEntity.setTag(new String[] {
                RandomUtil.randomString("活波开朗，具有进取精神和团队精神，有较强的动手能力。良好协调沟通能力，适应力强，反应快、积极、细心、灵活， 具有一定的社会交往能力", 3),
                RandomUtil.randomString("活波开朗，具有进取精神和团队精神，有较强的动手能力。良好协调沟通能力，适应力强，反应快、积极、细心、灵活， 具有一定的社会交往能力", 3),
                RandomUtil.randomString("活波开朗，具有进取精神和团队精神，有较强的动手能力。良好协调沟通能力，适应力强，反应快、积极、细心、灵活， 具有一定的社会交往能力", 3) });
        ElasticsearchEntity.setAddressLocation(address[RandomUtil.randomInt(0, address.length - 1)]);
        ElasticsearchEntity.setBirthAddress(city[RandomUtil.randomInt(0, city.length - 1)]);
        ElasticsearchEntity.setCreateTime(new Datetime());
        ElasticsearchEntity.setHasGirlFriend(id % 4 == 0 ? true : false);
        return ElasticsearchEntity;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public String[] getTag() {
        return tag;
    }

    public void setTag(String[] tag) {
        this.tag = tag;
    }

    public String getAddressLocation() {
        return addressLocation;
    }

    public void setAddressLocation(String addressLocation) {
        this.addressLocation = addressLocation;
    }

    public String getBirthAddress() {
        return birthAddress;
    }

    public void setBirthAddress(String birthAddress) {
        this.birthAddress = birthAddress;
    }

    public Datetime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Datetime createTime) {
        this.createTime = createTime;
    }

    public Boolean getHasGirlFriend() {
        return hasGirlFriend;
    }

    public void setHasGirlFriend(Boolean hasGirlFriend) {
        this.hasGirlFriend = hasGirlFriend;
    }

    public static String[] getCity() {
        return city;
    }

    public static String[] getAddress() {
        return address;
    }

    @Override
    public String toString() {
        return "ElasticsearchEntity [id=" + id + ", name=" + name + ", sex=" + sex + ", age=" + age + ", remark="
                + remark + ", tag=" + Arrays.toString(tag) + ", addressLocation=" + addressLocation + ", birthAddress="
                + birthAddress + ", createTime=" + createTime + ", hasGirlFriend=" + hasGirlFriend + "]";
    }

}
