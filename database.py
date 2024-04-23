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
    catalogNumber: Mapped[Optional[str]] = mapped_column(String(10000))
    coordinatePrecision: Mapped[Optional[str]] = mapped_column(String(10000))
    coordinateUncertaintyInMeters: Mapped[Optional[str]] = mapped_column(String(10000))
    created: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    dataGeneralizations: Mapped[Optional[bool]]
    datasetName: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    eventDate: Mapped[Optional[str]] = mapped_column(String(10000))
    group: Mapped[str] = mapped_column(String(50), index=True)
    tbiaID: Mapped[str] = mapped_column(String(50), index=True)
    vars()['license']: Mapped[Optional[str]] = mapped_column(String(10000))
    locality: Mapped[Optional[str]] = mapped_column(String(10000))
    location_rpt: Mapped[Optional[str]] = mapped_column(String(10000))
    raw_location_rpt: Mapped[Optional[str]] = mapped_column(String(10000))
    mediaLicense: Mapped[Optional[str]] = mapped_column(String(10000))
    modified: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    occurrenceID: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    organismQuantity: Mapped[Optional[str]] = mapped_column(String(10000))
    organismQuantityType: Mapped[Optional[str]] = mapped_column(String(10000))
    # originalScientificName: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    originalVernacularName: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    preservation: Mapped[Optional[str]] = mapped_column(String(10000))
    recordedBy: Mapped[Optional[str]] = mapped_column(String(10000))
    recordNumber: Mapped[Optional[str]] = mapped_column(String(10000))
    recordType: Mapped[Optional[str]] = mapped_column(String(10))
    references: Mapped[Optional[str]] = mapped_column(String(10000))
    resourceContacts: Mapped[Optional[str]] = mapped_column(String(10000))
    rightsHolder: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    selfProduced: Mapped[Optional[bool]]
    sensitiveCategory: Mapped[Optional[str]] = mapped_column(String(100))
    sourceCreated: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    sourceModified: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    sourceScientificName: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    sourceVernacularName: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    standardDate: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    standardLatitude: Mapped[Optional[float]]
    standardLongitude: Mapped[Optional[float]]
    standardRawLatitude: Mapped[Optional[float]]
    standardRawLongitude: Mapped[Optional[float]]
    standardOrganismQuantity: Mapped[Optional[float]]
    taxonID: Mapped[Optional[str]] = mapped_column(String(10), index=True)
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
    scientificNameID: Mapped[Optional[str]] = mapped_column(String(10000), index=True) # 原始資料提供
    # 為了因應某些單位有自己的學名系統 如TBN taxonUUID or TaiBIF的gbifAcceptedID
    sourceTaxonID: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    # 為了GBIF的資料從TaiBIF取得，留存TaiBIF的OccurrenceID於此，供更新使用(包含台博館&水利署)
    sourceOccurrenceID: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    # 保留未來學名比對使用
    sourceTaxonRank: Mapped[Optional[str]] = mapped_column(String(10000))
    sourceFamily: Mapped[Optional[str]] = mapped_column(String(10000))
    sourceOrder: Mapped[Optional[str]] = mapped_column(String(10000))
    sourceClass: Mapped[Optional[str]] = mapped_column(String(10000))
    # 日期改存年月日
    standardYear: Mapped[Optional[float]]
    standardMonth: Mapped[Optional[float]]
    standardDay: Mapped[Optional[float]]
    year: Mapped[Optional[str]] = mapped_column(String(50))
    month: Mapped[Optional[str]] = mapped_column(String(50))
    day: Mapped[Optional[str]] = mapped_column(String(50))
    # 資料是否刪除
    is_deleted: Mapped[bool] = mapped_column(server_default='f', index=True)
    sourceDatasetID: Mapped[Optional[str]] = mapped_column(String(10000), index=True, server_default='')
    tbiaDatasetID: Mapped[Optional[str]] = mapped_column(String(10000), index=True, server_default='') # 202404 目前是對到Dataset表的id

    __table_args__ = (
        UniqueConstraint('tbiaID',name='records_unique'),
    )




# 應deprecated
class Taxon(Base):
    __tablename__ = "taxon"

    id: Mapped[int] = mapped_column(primary_key=True)
    taxonID: Mapped[str] = mapped_column(String(10), index=True)
    # name_status:  Mapped[str] = mapped_column(String(20), index=True)
    aberration: Mapped[Optional[str]] = mapped_column(String(10000))
    aberration_c: Mapped[Optional[str]] = mapped_column(String(10000))
    alternative_name_c: Mapped[Optional[str]] = mapped_column(String(10000))
    vars()['class']: Mapped[Optional[str]] = mapped_column(String(10000))
    class_c: Mapped[Optional[str]] = mapped_column(String(10000))
    common_name_c: Mapped[Optional[str]] = mapped_column(String(10000))
    division: Mapped[Optional[str]] = mapped_column(String(10000))
    division_c: Mapped[Optional[str]] = mapped_column(String(10000))
    domain: Mapped[Optional[str]] = mapped_column(String(10000))
    domain_c: Mapped[Optional[str]] = mapped_column(String(10000))
    family: Mapped[Optional[str]] = mapped_column(String(10000))
    family_c: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_genus: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_subgenus: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_section: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_subsection: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_species: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_subspecies: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_nothosubspecies: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_variety: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_subvariety: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_nothovariety: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_form: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_subform: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_specialform: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_race: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_strip: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_morph: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_aberration: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_hybridformula: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_name: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_synonyms: Mapped[Optional[str]] = mapped_column(String(10000))
    formatted_misapplied: Mapped[Optional[str]] = mapped_column(String(10000))
    form: Mapped[Optional[str]] = mapped_column(String(10000))
    form_c: Mapped[Optional[str]] = mapped_column(String(10000))
    genus: Mapped[Optional[str]] = mapped_column(String(10000))
    genus_c: Mapped[Optional[str]] = mapped_column(String(10000))
    hybridformula: Mapped[Optional[str]] = mapped_column(String(10000))
    hybridformula_c: Mapped[Optional[str]] = mapped_column(String(10000))
    infraclass: Mapped[Optional[str]] = mapped_column(String(10000))
    infraclass_c: Mapped[Optional[str]] = mapped_column(String(10000))
    infradivision: Mapped[Optional[str]] = mapped_column(String(10000))
    infradivision_c: Mapped[Optional[str]] = mapped_column(String(10000))
    infrakingdom: Mapped[Optional[str]] = mapped_column(String(10000))
    infrakingdom_c: Mapped[Optional[str]] = mapped_column(String(10000))
    infraorder: Mapped[Optional[str]] = mapped_column(String(10000))
    infraorder_c: Mapped[Optional[str]] = mapped_column(String(10000))
    infraphylum: Mapped[Optional[str]] = mapped_column(String(10000))
    infraphylum_c: Mapped[Optional[str]] = mapped_column(String(10000))
    kingdom: Mapped[Optional[str]] = mapped_column(String(10000))
    kingdom_c: Mapped[Optional[str]] = mapped_column(String(10000))
    microphylum: Mapped[Optional[str]] = mapped_column(String(10000))
    microphylum_c: Mapped[Optional[str]] = mapped_column(String(10000))
    misapplied: Mapped[Optional[str]] = mapped_column(String(10000))
    morph: Mapped[Optional[str]] = mapped_column(String(10000))
    morph_c: Mapped[Optional[str]] = mapped_column(String(10000))
    name_author: Mapped[Optional[str]] = mapped_column(String(10000))
    nothosubspecies: Mapped[Optional[str]] = mapped_column(String(10000))
    nothosubspecies_c: Mapped[Optional[str]] = mapped_column(String(10000))
    nothovariety: Mapped[Optional[str]] = mapped_column(String(10000))
    nothovariety_c: Mapped[Optional[str]] = mapped_column(String(10000))
    order: Mapped[Optional[str]] = mapped_column(String(10000))
    order_c: Mapped[Optional[str]] = mapped_column(String(10000))
    parvdivision: Mapped[Optional[str]] = mapped_column(String(10000))
    parvdivision_c: Mapped[Optional[str]] = mapped_column(String(10000))
    parvphylum: Mapped[Optional[str]] = mapped_column(String(10000))
    parvphylum_c: Mapped[Optional[str]] = mapped_column(String(10000))
    phylum: Mapped[Optional[str]] = mapped_column(String(10000))
    phylum_c: Mapped[Optional[str]] = mapped_column(String(10000))
    race: Mapped[Optional[str]] = mapped_column(String(10000))
    race_c: Mapped[Optional[str]] = mapped_column(String(10000))
    scientificName: Mapped[Optional[str]] = mapped_column(String(10000))
    scientificNameID: Mapped[Optional[str]] = mapped_column(String(10000))
    section: Mapped[Optional[str]] = mapped_column(String(10000))
    section_c: Mapped[Optional[str]] = mapped_column(String(10000))
    specialform: Mapped[Optional[str]] = mapped_column(String(10000))
    specialform_c: Mapped[Optional[str]] = mapped_column(String(10000))
    species: Mapped[Optional[str]] = mapped_column(String(10000))
    species_c: Mapped[Optional[str]] = mapped_column(String(10000))
    stirp: Mapped[Optional[str]] = mapped_column(String(10000))
    stirp_c: Mapped[Optional[str]] = mapped_column(String(10000))
    subclass: Mapped[Optional[str]] = mapped_column(String(10000))
    subclass_c: Mapped[Optional[str]] = mapped_column(String(10000))
    subdivision: Mapped[Optional[str]] = mapped_column(String(10000))
    subdivision_c: Mapped[Optional[str]] = mapped_column(String(10000))
    subfamily: Mapped[Optional[str]] = mapped_column(String(10000))
    subfamily_c: Mapped[Optional[str]] = mapped_column(String(10000))
    subform: Mapped[Optional[str]] = mapped_column(String(10000))
    subform_c: Mapped[Optional[str]] = mapped_column(String(10000))
    subgenus: Mapped[Optional[str]] = mapped_column(String(10000))
    subgenus_c: Mapped[Optional[str]] = mapped_column(String(10000))
    subkingdom: Mapped[Optional[str]] = mapped_column(String(10000))
    subkingdom_c: Mapped[Optional[str]] = mapped_column(String(10000))
    suborder: Mapped[Optional[str]] = mapped_column(String(10000))
    suborder_c: Mapped[Optional[str]] = mapped_column(String(10000))
    subphylum: Mapped[Optional[str]] = mapped_column(String(10000))
    subphylum_c: Mapped[Optional[str]] = mapped_column(String(10000))
    subsection: Mapped[Optional[str]] = mapped_column(String(10000))
    subsection_c: Mapped[Optional[str]] = mapped_column(String(10000))
    subspecies: Mapped[Optional[str]] = mapped_column(String(10000))
    subspecies_c: Mapped[Optional[str]] = mapped_column(String(10000))
    subtribe: Mapped[Optional[str]] = mapped_column(String(10000))
    subtribe_c: Mapped[Optional[str]] = mapped_column(String(10000))
    subvariety: Mapped[Optional[str]] = mapped_column(String(10000))
    subvariety_c: Mapped[Optional[str]] = mapped_column(String(10000))
    superclass: Mapped[Optional[str]] = mapped_column(String(10000))
    superclass_c: Mapped[Optional[str]] = mapped_column(String(10000))
    superdivision: Mapped[Optional[str]] = mapped_column(String(10000))
    superdivision_c: Mapped[Optional[str]] = mapped_column(String(10000))
    superfamily: Mapped[Optional[str]] = mapped_column(String(10000))
    superfamily_c: Mapped[Optional[str]] = mapped_column(String(10000))
    superkingdom: Mapped[Optional[str]] = mapped_column(String(10000))
    superkingdom_c: Mapped[Optional[str]] = mapped_column(String(10000))
    superorder: Mapped[Optional[str]] = mapped_column(String(10000))
    superorder_c: Mapped[Optional[str]] = mapped_column(String(10000))
    superphylum: Mapped[Optional[str]] = mapped_column(String(10000))
    superphylum_c: Mapped[Optional[str]] = mapped_column(String(10000))
    synonyms: Mapped[Optional[str]] = mapped_column(String(10000))
    taxonRank: Mapped[Optional[str]] = mapped_column(String(10000))
    tribe: Mapped[Optional[str]] = mapped_column(String(10000))
    tribe_c: Mapped[Optional[str]] = mapped_column(String(10000))
    variety: Mapped[Optional[str]] = mapped_column(String(10000))
    variety_c: Mapped[Optional[str]] = mapped_column(String(10000))
    cites: Mapped[Optional[str]] = mapped_column(String(10000))
    iucn: Mapped[Optional[str]] = mapped_column(String(10000))
    redlist: Mapped[Optional[str]] = mapped_column(String(10000))
    protected: Mapped[Optional[str]] = mapped_column(String(10000))
    sensitive: Mapped[Optional[str]] = mapped_column(String(10000))
    alien_type: Mapped[Optional[str]] = mapped_column(String(10000))
    is_endemic: Mapped[Optional[bool]]
    is_fossil: Mapped[Optional[bool]]
    is_terrestrial: Mapped[Optional[bool]]
    is_freshwater: Mapped[Optional[bool]]
    is_brackish: Mapped[Optional[bool]]
    is_marine: Mapped[Optional[bool]]


# \copy data_taxon ("taxonID", "scientificNameID", "scientificName", "name_author", "formatted_name", "common_name_c", "alternative_name_c", "synonyms", "formatted_synonyms", "misapplied", "formatted_misapplied", "kingdom", "kingdom_c", "subkingdom", "subkingdom_c", "infrakingdom", "infrakingdom_c", "superphylum", "superphylum_c", "phylum", "phylum_c", "subphylum", "subphylum_c", "infraphylum", "infraphylum_c", "superclass", "superclass_c", "class", "class_c", "subclass", "subclass_c", "superorder", "superorder_c", "order", "order_c", "family", "family_c", "genus", "genus_c", "formatted_genus", "subfamily", "subfamily_c", "infraclass", "infraclass_c", "superfamily", "superfamily_c", "species", "species_c", "formatted_species", "subspecies", "subspecies_c", "formatted_subspecies", "infraorder", "infraorder_c", "tribe", "tribe_c", "subgenus", "subgenus_c", "formatted_subgenus", "subtribe", "subtribe_c", "section", "section_c", "formatted_section", "subsection", "subsection_c", "formatted_subsection", "variety", "variety_c", "formatted_variety", "taxonRank") FROM '/tbia-volumes/bucket/source_taicol_for_tbia_20230904.csv' csv header;


class DeletedRecords(Base):
    __tablename__ = "deleted_records"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    tbiaID: Mapped[str] = mapped_column(String(50), index=True)
    group: Mapped[str] = mapped_column(String(50), index=True)
    rights_holder:  Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    occurrenceID: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    deleted: Mapped[datetime] = mapped_column(DateTime(timezone=True)) # 刪除時間



class Dataset(Base):
    __tablename__ = "dataset"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(10000), index=True)
    datasetTaxonGroup: Mapped[Optional[str]] = mapped_column(String(10000))
    resourceContacts: Mapped[Optional[str]] = mapped_column(String(10000))
    record_type: Mapped[Optional[str]] = mapped_column(String(20), index=True) # 多值以逗號合併
    rights_holder:  Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    deprecated: Mapped[bool] = mapped_column(server_default='f', index=True)
    sourceDatasetID: Mapped[Optional[str]] = mapped_column(String(10000), index=True, server_default='')
    gbifDatasetID: Mapped[Optional[str]] = mapped_column(String(10000), index=True, server_default='')
    tbiaDatasetID: Mapped[Optional[str]] = mapped_column(String(10000), index=True, server_default='') 
    dateCoverage: Mapped[Optional[str]] = mapped_column(String(10000))
    occurrenceCount: Mapped[Optional[int]] = mapped_column(server_default='0')
    datasetAuthor: Mapped[Optional[str]] = mapped_column(String(10000))
    datasetURL: Mapped[Optional[str]] = mapped_column(String(10000))
    datasetLicense: Mapped[Optional[str]] = mapped_column(String(10000))
    datasetPublisher: Mapped[Optional[str]] = mapped_column(String(10000))
    created: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    modified: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    update_version: Mapped[Optional[int]] = mapped_column(server_default='0', index=True)

    __table_args__ = (
        UniqueConstraint('name','record_type','rights_holder','sourceDatasetID', name='dataset_unique'),
    )


# 串TaiEOL 取得物種照片
class SpeciesImages(Base):
    __tablename__ = "species_images"
    
    id: Mapped[int] = mapped_column(primary_key=True)
    taxon_name_id: Mapped[Optional[str]] = mapped_column(String(1000))
    namecode: Mapped[Optional[str]] = mapped_column(String(1000))
    taieol_id: Mapped[Optional[str]] = mapped_column(String(1000))
    images: Mapped[Optional[str]] = mapped_column(JSON)
    modified: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint('namecode','taxon_name_id', name='namecode'),
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
