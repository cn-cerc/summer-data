package cn.cerc.core;

import java.io.Serializable;

import cn.cerc.db.editor.GetSetTextEvent;
import cn.cerc.db.editor.GetTextEvent;
import cn.cerc.db.editor.SetTextEvent;

public final class FieldMeta implements Serializable {
    private static final long serialVersionUID = -6898050783447062943L;
    private String _code;
    private String _name;
    private FieldType _type;
    private FieldKind _kind;
    private String _remark;
    private boolean _updateKey;
    private boolean _autoincrement;
    private GetTextEvent _onGetTextEvent;
    private SetTextEvent _onSetTextEvent;

    public enum FieldKind {
        Memory, Storage, Calculated;
    }

    public FieldMeta(String code) {
        super();
        if (code == null || "".equals(code))
            throw new RuntimeException("fieldCode is null!");
        this._code = code;
        this._kind = FieldKind.Memory;
    }

    public FieldMeta(String code, FieldKind kind) {
        super();
        if (code == null || "".equals(code))
            throw new RuntimeException("fieldCode is null!");
        if (kind == null)
            throw new RuntimeException("fieldKind is null!");
        this._code = code;
        this._kind = kind;
    }

    @Override
    public FieldMeta clone() {
        FieldMeta result = new FieldMeta(this._code);
        result._name = this._name;
        if (this._type != null)
            result._type = this._type.clone();
        result._kind = this._kind;
        result._remark = this._remark;
        result._updateKey = this._updateKey;
        result._autoincrement = this._autoincrement;
        result._onGetTextEvent = this._onGetTextEvent;
        result._onSetTextEvent = this._onSetTextEvent;
        return result;
    }

    public final String getCode() {
        return _code;
    }

    public final String getName() {
        return _name;
    }

    public final FieldMeta setName(String name) {
        this._name = name;
        return this;
    }

    public final String getType() {
        return _type == null ? null : _type.toString();
    }

    public final FieldMeta setType(String type) {
        if (type == null) {
            this._type = null;
        } else {
            if (this._type == null)
                this._type = new FieldType();
            this._type.setType(type);
        }
        return this;
    }

    public final FieldType setType(Class<?> clazz) {
        return getFieldType().setType(clazz);
    }

    public final FieldType setType(Class<?> clazz, int length) {
        return getFieldType().setType(clazz).setLength(length);
    }

    public final FieldType getFieldType() {
        if (this._type == null)
            this._type = new FieldType();
        return _type;
    }

    public final FieldKind getKind() {
        return _kind;
    }

    public final FieldMeta setKind(FieldKind type) {
        if (type == null)
            throw new RuntimeException("fieldKind is null!");
        if (type == FieldKind.Storage)
            throw new RuntimeException("Wrong direction of modification");
        this._kind = type;
        return this;
    }

    public final boolean isUpdateKey() {
        return _updateKey;
    }

    public final String getRemark() {
        return _remark;
    }

    public final FieldMeta setRemark(String remark) {
        this._remark = remark;
        return this;
    }

    public final FieldMeta setUpdateKey(boolean updateKey) {
        this._updateKey = updateKey;
        return this;
    }

    public final boolean isAutoincrement() {
        return _autoincrement;
    }

    public final FieldMeta setAutoincrement(boolean autoincrement) {
        this._autoincrement = autoincrement;
        return this;
    }

    @Override
    public final int hashCode() {
        return _code.hashCode();
    }

    @Override
    public final boolean equals(Object anObject) {
        if (this == anObject) {
            return true;
        }
        if (anObject instanceof FieldMeta) {
            FieldMeta meta = (FieldMeta) anObject;
            return _code.equals(meta.getCode());
        }
        return false;
    }

    public FieldMeta onGetText(GetTextEvent getTextEvent) {
        this._onGetTextEvent = getTextEvent;
        return this;
    }

    public FieldMeta onSetText(SetTextEvent setTextEvent) {
        this._onSetTextEvent = setTextEvent;
        return this;
    }

    public String getText(DataRow record) {
        if (_onGetTextEvent == null)
            return record.getString(_code);
        return _onGetTextEvent.getText(record, this);
    }

    public Object setText(String value) {
        if (_onSetTextEvent == null)
            return value;
        return _onSetTextEvent.setText(value);
    }

    public void onGetSetText(GetSetTextEvent getsetTextEvent) {
        this.onGetText(getsetTextEvent);
        this.onSetText(getsetTextEvent);
    }
}
