package cn.cerc.core;

import cn.cerc.core.DataSet.DataSetAfterDeleteEvent;
import cn.cerc.core.DataSet.DataSetAfterUpdateEvent;
import cn.cerc.core.DataSet.DataSetBeforeDeleteEvent;
import cn.cerc.core.DataSet.DataSetBeforeUpdateEvent;
import cn.cerc.core.DataSet.DataSetInsertEvent;

public interface DataSetEvent extends DataSetInsertEvent, DataSetBeforeUpdateEvent, DataSetAfterUpdateEvent,
        DataSetBeforeDeleteEvent, DataSetAfterDeleteEvent {

}
