from typing import List
from typing import Optional
from sqlalchemy import UniqueConstraint
from sqlalchemy import String, DateTime, JSON
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from datetime import datetime

class Base(DeclarativeBase):
    pass


class MatchLog(Base):
    __tablename__ = "match_log"

    id: Mapped[int] = mapped_column(primary_key=True)
    occurrenceID: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    catalogNumber: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    tbiaID: Mapped[str] = mapped_column(String(50), index=True)
    group: Mapped[str] = mapped_column(String(50), index=True)
    rights_holder:  Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    sourceScientificName: Mapped[Optional[str]] = mapped_column(String(10000))
    is_matched: Mapped[bool]

    taxonID: Mapped[Optional[str]] = mapped_column(String(10), index=True)
    # parentTaxonID: Mapped[Optional[str]] = mapped_column(String(10), index=True)
    match_higher_taxon: Mapped[bool] = mapped_column(server_default='f')

    match_stage: Mapped[Optional[int]]
    stage_1: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    stage_2: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    stage_3: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    stage_4: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    stage_5: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    stage_6: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    stage_7: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    stage_8: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    created: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    modified: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    
    __table_args__ = (
        UniqueConstraint('tbiaID', name='matchlog_unique'),
    )


# !! 注意 如果修改欄位名稱的話 會讓原本的欄位移除 新的欄位會是空值

class Records(Base):
    __tablename__ = "records"

    id: Mapped[int] = mapped_column(primary_key=True)
    match_higher_taxon: Mapped[bool] = mapped_column(server_default='f')
    update_version: Mapped[Optional[int]] = mapped_column(server_default='0', index=True)
    associatedMedia: Mapped[Optional[str]] = mapped_column(String(10000))
    basisOfRecord: Mapped[Optional[str]] = mapped_column(String(10000))
    catalogNumber: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    coordinatePrecision: Mapped[Optional[str]] = mapped_column(String(10000))
    coordinateUncertaintyInMeters: Mapped[Optional[str]] = mapped_column(String(10000))
    created: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    dataGeneralizations: Mapped[Optional[bool]]
    datasetName: Mapped[Optional[str]] = mapped_column(String(10000))
    eventDate: Mapped[Optional[str]] = mapped_column(String(10000))
    group: Mapped[str] = mapped_column(String(50), index=True)
    tbiaID: Mapped[str] = mapped_column(String(50), index=True)
    vars()['license']: Mapped[Optional[str]] = mapped_column(String(10000))
    locality: Mapped[Optional[str]] = mapped_column(String(10000))
    location_rpt: Mapped[Optional[str]] = mapped_column(String(10000))
    raw_location_rpt: Mapped[Optional[str]] = mapped_column(String(10000))
    mediaLicense: Mapped[Optional[str]] = mapped_column(String(10000))
    modified: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    occurrenceID: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    organismQuantity: Mapped[Optional[str]] = mapped_column(String(10000))
    organismQuantityType: Mapped[Optional[str]] = mapped_column(String(10000))
    # originalScientificName: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    originalVernacularName: Mapped[Optional[str]] = mapped_column(String(10000))
    preservation: Mapped[Optional[str]] = mapped_column(String(10000))
    recordedBy: Mapped[Optional[str]] = mapped_column(String(10000))
    recordNumber: Mapped[Optional[str]] = mapped_column(String(10000))
    recordType: Mapped[Optional[str]] = mapped_column(String(10))
    references: Mapped[Optional[str]] = mapped_column(String(10000))
    resourceContacts: Mapped[Optional[str]] = mapped_column(String(10000))
    rightsHolder: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    selfProduced: Mapped[Optional[bool]]
    sensitiveCategory: Mapped[Optional[str]] = mapped_column(String(100))
    sourceCreated: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    sourceModified: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    sourceScientificName: Mapped[Optional[str]] = mapped_column(String(10000))
    sourceVernacularName: Mapped[Optional[str]] = mapped_column(String(10000))
    standardDate: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    standardLatitude: Mapped[Optional[float]]
    standardLongitude: Mapped[Optional[float]]
    standardRawLatitude: Mapped[Optional[float]]
    standardRawLongitude: Mapped[Optional[float]]
    standardOrganismQuantity: Mapped[Optional[float]]
    taxonID: Mapped[Optional[str]] = mapped_column(String(10))
    # parentTaxonID: Mapped[Optional[str]] = mapped_column(String(10), index=True)
    typeStatus: Mapped[Optional[str]] = mapped_column(String(10000))
    verbatimCoordinateSystem: Mapped[Optional[str]] = mapped_column(String(10000))
    verbatimLatitude: Mapped[Optional[str]] = mapped_column(String(10000))
    verbatimLongitude: Mapped[Optional[str]] = mapped_column(String(10000))
    verbatimRawLatitude: Mapped[Optional[str]] = mapped_column(String(10000))
    verbatimRawLongitude: Mapped[Optional[str]] = mapped_column(String(10000))
    verbatimSRS: Mapped[Optional[str]] = mapped_column(String(10000))
    # 未模糊化網格
    grid_1: Mapped[Optional[str]] = mapped_column(String(50))
    grid_5: Mapped[Optional[str]] = mapped_column(String(50))
    grid_10: Mapped[Optional[str]] = mapped_column(String(50))
    grid_100: Mapped[Optional[str]] = mapped_column(String(50))
    # 模糊化網格
    grid_1_blurred: Mapped[Optional[str]] = mapped_column(String(50))
    grid_5_blurred: Mapped[Optional[str]] = mapped_column(String(50))
    grid_10_blurred: Mapped[Optional[str]] = mapped_column(String(50))
    grid_100_blurred: Mapped[Optional[str]] = mapped_column(String(50))
    scientificNameID: Mapped[Optional[str]] = mapped_column(String(10000)) # 原始資料提供
    # 為了因應某些單位有自己的學名系統 如TBN taxonUUID or TaiBIF的gbifAcceptedID
    sourceTaxonID: Mapped[Optional[str]] = mapped_column(String(10000))
    # 為了GBIF的資料從TaiBIF取得，留存TaiBIF的OccurrenceID於此，供更新使用(包含台博館&水利署)
    sourceOccurrenceID: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    # 保留未來學名比對使用
    sourceTaxonRank: Mapped[Optional[str]] = mapped_column(String(10000))
    sourceFamily: Mapped[Optional[str]] = mapped_column(String(10000))
    sourceOrder: Mapped[Optional[str]] = mapped_column(String(10000))
    sourceClass: Mapped[Optional[str]] = mapped_column(String(10000))
    # # 日期改存年月日 (pending) 先拿掉
    # standardYear: Mapped[Optional[float]]
    # standardMonth: Mapped[Optional[float]]
    # standardDay: Mapped[Optional[float]]
    # town: Mapped[Optional[str]] = mapped_column(String(50))
    county: Mapped[Optional[str]] = mapped_column(String(50))
    municipality: Mapped[Optional[str]] = mapped_column(String(50))
    rawCounty: Mapped[Optional[str]] = mapped_column(String(50))
    rawMunicipality: Mapped[Optional[str]] = mapped_column(String(50))
    # raw_county: Mapped[Optional[str]] = mapped_column(String(50))
    # raw_town: Mapped[Optional[str]] = mapped_column(String(50))
    year: Mapped[Optional[str]] = mapped_column(String(50))
    month: Mapped[Optional[str]] = mapped_column(String(50))
    day: Mapped[Optional[str]] = mapped_column(String(50))
    # 資料是否刪除 # 下次應該可以拿掉
    is_deleted: Mapped[bool] = mapped_column(server_default='f') 
    sourceDatasetID: Mapped[Optional[str]] = mapped_column(String(10000), index=True, server_default='')
    tbiaDatasetID: Mapped[Optional[str]] = mapped_column(String(10000), index=True, server_default='') # 202404 目前是對到Dataset表的id

    dataQuality: Mapped[Optional[int]]

    __table_args__ = (
        UniqueConstraint('tbiaID',name='records_unique'),
    )


class DeletedRecords(Base):
    __tablename__ = "deleted_records"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    tbiaID: Mapped[str] = mapped_column(String(50), index=True)
    group: Mapped[str] = mapped_column(String(50), index=True)
    rights_holder:  Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    occurrenceID: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    catalogNumber: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    deleted: Mapped[datetime] = mapped_column(DateTime(timezone=True)) # 刪除時間



class Dataset(Base):
    __tablename__ = "dataset"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(10000), index=True)
    group: Mapped[Optional[str]] = mapped_column(String(50), index=True)
    record_type: Mapped[Optional[str]] = mapped_column(String(20), index=True) # 多值以逗號合併
    rights_holder:  Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    deprecated: Mapped[bool] = mapped_column(server_default='f', index=True)
    
    sourceDatasetID: Mapped[Optional[str]] = mapped_column(String(10000), index=True, server_default='')
    gbifDatasetID: Mapped[Optional[str]] = mapped_column(String(10000), index=True, server_default='')
    tbiaDatasetID: Mapped[Optional[str]] = mapped_column(String(10000), index=True, server_default='') 

    resourceContacts: Mapped[Optional[str]] = mapped_column(String(10000))
    datasetDateStart: Mapped[Optional[str]] = mapped_column(String(10000)) # 涵蓋時間 - 始
    datasetDateEnd: Mapped[Optional[str]] = mapped_column(String(10000)) # 涵蓋時間 - 末
    occurrenceCount: Mapped[Optional[int]] = mapped_column(server_default='0')

    datasetURL: Mapped[Optional[str]] = mapped_column(String(10000))
    datasetLicense: Mapped[Optional[str]] = mapped_column(String(10000))
    datasetPublisher: Mapped[Optional[str]] = mapped_column(String(10000))
    datasetTaxonGroup: Mapped[Optional[str]] = mapped_column(String(10000)) # 只存類群文字
    datasetTaxonStat: Mapped[Optional[str]] = mapped_column(JSON) # 包含類群+count

    created: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    modified: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    update_version: Mapped[Optional[int]] = mapped_column(server_default='0', index=True)
    downloadCount: Mapped[Optional[int]] = mapped_column(server_default='0')

    __table_args__ = (
        UniqueConstraint('name','rights_holder','sourceDatasetID', name='dataset_unique'),
    )


# 串TaiEOL 取得物種照片
class SpeciesImages(Base):
    __tablename__ = "species_images"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    taxon_name_id: Mapped[Optional[str]] = mapped_column(String(1000))
    namecode: Mapped[Optional[str]] = mapped_column(String(1000))
    taieol_id: Mapped[Optional[str]] = mapped_column(String(1000))
    taieol_url: Mapped[Optional[str]] = mapped_column(String(10000)) # 連接至taieol 物種頁
    taxon_id: Mapped[Optional[str]] = mapped_column(String(20))
    images: Mapped[Optional[str]] = mapped_column(JSON)
    modified: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint('taxon_id', name='taxon_images'),
    )


# 資料更新紀錄
class UpdateVersion(Base):
    __tablename__ = "update_version"
    
    id: Mapped[int] = mapped_column(primary_key=True)

    update_version: Mapped[Optional[int]]
    current_page: Mapped[Optional[int]]
    rights_holder:  Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    note: Mapped[Optional[str]] = mapped_column(JSON)
    is_finished: Mapped[bool] = mapped_column(server_default='f')
    created: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    modified: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint('rights_holder','update_version', name='update_unique'),
    )


# 影像來源規則
class MediaRule(Base):
    __tablename__ = "media_rule"
    
    id: Mapped[int] = mapped_column(primary_key=True)

    rights_holder:  Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    media_rule: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    modified: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint('rights_holder','media_rule', name='media_rule_unique'),
    )
