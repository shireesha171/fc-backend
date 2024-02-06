
from .snspublisher import get_current_datatime

def printlogs(record,logger):
    file_config_information(record['file_config_details'],logger)
    standard_validation(record['standard_validations'], logger)
    source_details(record,logger)
    target_details(record,logger)
    dq_rules(record['dqrules'],logger)
    metadata(record['metadata'], logger)
    source_information(record,logger)
    target_information(record,logger)


def source_information(record,logger):
    logger.info("getting source config file information " + get_current_datatime())
    source = {
        "name": record['source_name'],
        "connectivity_type": record['source_connectivity_type'],
        "location_pattern": record['source_location_pattern'],
        "absolute_file_path": record['source_absolute_file_path']

    }
    logger.info("Object: %s", source)


def target_information(record, logger):
    logger.info("getting target config file information " + get_current_datatime())
    target = {
        "name": record['source_name'],
        "connectivity_type": record['target_connectivity_type'],
        "location_pattern": record['target_location_pattern'],
        "absolute_file_path": record['target_absolute_file_path']
    }
    logger.info("Object: %s", target)
def file_config_information(source,logger):
    logger.info("getting file_configuration "+ get_current_datatime())
    logger.info("Object: %s", source )

def dq_rules(dqrules,logger):
    logger.info("getting dq Rules "+ get_current_datatime())
    logger.info("Object: %s", dqrules )

def standard_validation(validation,logger):
    logger.info("getting validation " + get_current_datatime())
    logger.info("Object: %s", validation )
# def get_current_datatime():
#         message = "Current datetime: " + str(datetime.datetime.now())
#         return message

def metadata(metadata_information,logger):
    logger.info("getting  source-file metadata information" + get_current_datatime())
    for item in metadata_information:
       logger.info("Object: %s", item)

def error_logs(error_list,logger):
    logger.info("printing  success and error information" + get_current_datatime())
    for item in error_list:
        del item['status_type']
        logger.info("Object: %s", item)

def source_details(record,logger):
    logger.info("printing  source Details information" + get_current_datatime())
    source = {
        "source_name": record['source_name'],
        "source_location_pattern": record['source_location_pattern'],
        "source_absolute_file_path": record['source_absolute_file_path'],
        "source_absolute_file_path": record['source_absolute_file_path']
    }
    logger.info("Object: %s", source)

def target_details(record,logger):
    logger.info("printing  target Details information" + get_current_datatime())
    target = {
        "target_name": record['target_name'],
        "target_location_pattern": record['target_location_pattern'],
        "target_absolute_file_path": record['target_absolute_file_path'],
        "target_connectivity_type": record['target_connectivity_type']
    }
    logger.info("Object: %s", target)