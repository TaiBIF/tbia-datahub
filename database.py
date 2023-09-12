from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String, DateTime
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
    occurrenceID: Mapped[Optional[str]] = mapped_column(String(1000), index=True)
    tbiaID: Mapped[str] = mapped_column(String(50), index=True)
    group: Mapped[str] = mapped_column(String(50), index=True)
    sourceScientificName: Mapped[Optional[str]] = mapped_column(String(1000))
    is_matched: Mapped[bool]

    taxonID: Mapped[Optional[str]] = mapped_column(String(10), index=True)
    parentTaxonID: Mapped[Optional[str]] = mapped_column(String(10), index=True)

    match_stage: Mapped[Optional[int]]
    stage_1: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    stage_2: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    stage_3: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    stage_4: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    stage_5: Mapped[Optional[str]] = mapped_column(String(20), index=True)
    created: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    modified: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class Records(Base):
    __tablename__ = "records"

    id: Mapped[int] = mapped_column(primary_key=True)
    associatedMedia: Mapped[Optional[str]] = mapped_column(String(10000))
    basisOfRecord: Mapped[Optional[str]] = mapped_column(String(1000))
    collectionID: Mapped[Optional[str]] = mapped_column(String(10000))
    coordinatePrecision: Mapped[Optional[str]] = mapped_column(String(1000))
    coordinateUncertaintyInMeters: Mapped[Optional[str]] = mapped_column(String(1000))
    created: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    dataGeneralizations: Mapped[Optional[bool]]
    datasetName: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
    eventDate: Mapped[Optional[str]] = mapped_column(String(1000))
    group: Mapped[str] = mapped_column(String(50), index=True)
    tbiaID: Mapped[str] = mapped_column(String(50), index=True)
    vars()['license']: Mapped[Optional[str]] = mapped_column(String(10000))
    locality: Mapped[Optional[str]] = mapped_column(String(10000))
    location_rpt: Mapped[Optional[str]] = mapped_column(String(1000))
    raw_location_rpt: Mapped[Optional[str]] = mapped_column(String(1000))
    mediaLicense: Mapped[Optional[str]] = mapped_column(String(10000))
    modified: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    occurrenceID: Mapped[Optional[str]] = mapped_column(String(1000), index=True)
    organismQuantity: Mapped[Optional[str]] = mapped_column(String(1000))
    organismQuantityType: Mapped[Optional[str]] = mapped_column(String(1000))
    originalScientificName: Mapped[Optional[str]] = mapped_column(String(10000), index=True)
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
    standardLatitude = Mapped[Optional[float]]
    standardLongitude: Mapped[Optional[float]]
    standardRawLatitude: Mapped[Optional[float]]
    standardRawLongitude: Mapped[Optional[float]]
    standardOrganismQuantity: Mapped[Optional[float]]
    taxonID: Mapped[Optional[str]] = mapped_column(String(10), index=True)
    parentTaxonID: Mapped[Optional[str]] = mapped_column(String(10), index=True)
    typeStatus: Mapped[Optional[str]] = mapped_column(String(10000))
    verbatimCoordinateSystem: Mapped[Optional[str]] = mapped_column(String(10000))
    verbatimLatitude: Mapped[Optional[str]] = mapped_column(String(10000))
    verbatimLongitude: Mapped[Optional[str]] = mapped_column(String(10000))
    verbatimRawLatitude: Mapped[Optional[str]] = mapped_column(String(10000))
    verbatimRawLongitude: Mapped[Optional[str]] = mapped_column(String(10000))
    verbatimSRS: Mapped[Optional[str]] = mapped_column(String(10000))
    grid_1: Mapped[Optional[str]] = mapped_column(String(50))
    grid_5: Mapped[Optional[str]] = mapped_column(String(50))
    grid_10: Mapped[Optional[str]] = mapped_column(String(50))
    grid_100: Mapped[Optional[str]] = mapped_column(String(50))
    scientificNameID: Mapped[Optional[str]] = mapped_column(String(1000), index=True)
    # 為了因應某些單位有自己的學名系統 如TBN taxonUUID
    sourceTaxonID: Mapped[Optional[str]] = mapped_column(String(1000), index=True)
