package cn.cerc.db.core;

import cn.cerc.db.core.DataSet.DataSetAfterDeleteEvent;
import cn.cerc.db.core.DataSet.DataSetAfterUpdateEvent;
import cn.cerc.db.core.DataSet.DataSetBeforeDeleteEvent;
import cn.cerc.db.core.DataSet.DataSetBeforeUpdateEvent;
import cn.cerc.db.core.DataSet.DataSetInsertEvent;

public interface DataSetEvent extends DataSetInsertEvent, DataSetBeforeUpdateEvent, DataSetAfterUpdateEvent,
        DataSetBeforeDeleteEvent, DataSetAfterDeleteEvent {

}
