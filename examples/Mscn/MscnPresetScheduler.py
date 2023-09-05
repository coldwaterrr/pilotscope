import sys
sys.path.append("../")
import unittest

from pilotscope.DataManager.PilotTrainDataManager import PilotTrainDataManager
from pilotscope.Factory.SchedulerFactory import SchedulerFactory
from pilotscope.common.Util import pilotscope_exit
from pilotscope.DataFetcher.PilotDataInteractor import PilotDataInteractor
from pilotscope.PilotConfig import PilotConfig, PostgreSQLConfig
from pilotscope.PilotEnum import DatabaseEnum, EventEnum
from pilotscope.PilotModel import PilotModel
from pilotscope.PilotScheduler import PilotScheduler
from examples.Mscn.EventImplement import MscnPretrainingModelEvent
from examples.Mscn.MscnParadigmCardAnchorHandler import MscnParadigmCardAnchorHandler
from examples.Mscn.MscnPilotModel import MscnPilotModel
from examples.utils import load_sql,load_test_sql

def get_mscn_preset_scheduler(config, enable_collection, enable_training) -> PilotScheduler:
    if type(enable_collection) == str:
        enable_collection = eval(enable_collection)
    if type(enable_training) == str:
        enable_training = eval(enable_training)
    model_name = "mscn"
    mscn_pilot_model: PilotModel = MscnPilotModel(model_name)
    mscn_pilot_model.load()
    mscn_handler = MscnParadigmCardAnchorHandler(mscn_pilot_model, config)

    # Register what data needs to be cached for training purposes
    data_interactor = PilotDataInteractor(config)
    data_interactor.pull_execution_time()

    # core
    test_data_save_table = "{}_data_table".format(model_name)
    pretrain_data_save_table = "{}_pretrain_data_table2".format(model_name)
    scheduler: PilotScheduler = SchedulerFactory.get_pilot_scheduler(config)
    scheduler.register_anchor_handler(mscn_handler)
    scheduler.register_collect_data(training_data_save_table=test_data_save_table,
                                    data_interactor=data_interactor)
    # allow to pretrain model           
    pretraining_event = MscnPretrainingModelEvent(config, mscn_pilot_model, pretrain_data_save_table, 
                                                    enable_collection=enable_collection,
                                                    enable_training=enable_training,
                                                    training_data_file=None)
                                                # If training_data_file is None, training data will be collected in this run
    scheduler.register_event(EventEnum.PRETRAINING_EVENT, pretraining_event)

    # start
    scheduler.init()
    return scheduler