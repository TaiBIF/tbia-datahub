# 取得dataset資訊
import requests
import pandas as pd
import numpy as np
import bson
from app import SessionLocal
from models import Dataset  
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert


def fetch_tbn_datasets(modified_since: str = '1900-01-01') -> pd.DataFrame:
    """從 TBN API 分頁取得 dataset 清單。

    Args:
        modified_since: 只取此日期之後修改過的 dataset，預設抓全部。

    Returns:
        包含所有 dataset 資訊的 DataFrame；失敗時回傳已取得的部分。
    """

    # 自產資料 + eBird
    url_list = ['https://www.tbn.org.tw/api/v25/occurrence?selfProduced=y', 'https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=4fa7b334-ce0d-4e88-aaae-2e0c138d049e']

    # 從ipt上傳的tbri資料
    url_list += ["https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=4410edca-3bdd-4475-98a2-de823b2266bc",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=e0b8cb67-6667-423d-ab71-08021b6485f3",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=f170f056-3f8a-4ef3-ac9f-4503cc854ce0",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=4daa291b-0e9d-4e21-b78d-6b4e96093adc",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=f3f25fcf-2930-4cf1-a495-6b31d7fa0252",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=3f9cd7e5-6d7b-40a8-8062-a18d2f2ca599",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=db09684b-0fd1-431e-b5fa-4c1532fbdb14",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=54eaea55-f346-442e-9414-039c25658877",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=617e5387-3122-47b7-b639-c9fafc35bf13",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=346c95be-c7b3-41dc-99c9-e88a18d8884a",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=f464cad8-531e-4d53-ad36-2e4430f6765e",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=cb6e756a-c56a-4dc4-bbfa-2002a0a754dd",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=cb382c4d-7b6c-40c2-9e2d-e8167380cec5",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=0528b82f-bebb-49b0-ad2e-5082ae002823",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=a1f3b9e3-60d5-49fe-a6d1-2d22a154e2b2",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=7bff8305-a1e3-4e5b-bbc3-4afe04006b88",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=3a3aae4c-5895-4ba5-b3ba-d5f7d924478d",
            "https://www.tbn.org.tw/api/v25/occurrence?datasetUUID=6ef6360c-c904-4eab-87fe-7bd234cb5c42",
            ]
    
    url = f'https://www.tbn.org.tw/api/v25/dataset?modified={modified_since}'
    records = []
    while url:
        resp = requests.get(url)
        if resp.status_code != 200:
            print(f'[fetch_tbn_datasets] 中斷於 {url}, status={resp.status_code}')
            break
        payload = resp.json()
        records += payload.get('data', [])
        url = payload.get('links', {}).get('next')

    return url_list, pd.DataFrame(records)


def fetch_taibif_datasets(
    publisher_id=None,
    source=None,                    # 'GBIF' or 'not_GBIF' or None
    exclude_publishers=None,        # list of publisher_ids
    exclude_datasets=None,          # list of taibifDatasetID
    exclude_gbif_datasets=None,     # list of gbifDatasetID
    only_tw_publishers=False,       # 是否限定台灣發布者
):
    # 取得資料集
    url = "https://portal.taibif.tw/api/v3/dataset"
    if publisher_id:
        url += f"?publisherID={publisher_id}"
    response = requests.get(url)
    if response.status_code != 200:
        return pd.DataFrame(), []

    data = response.json()['data']
    dataset = pd.DataFrame(data)

    # core 過濾（單一 publisher 情境原本是在產 dataset_list 時才篩，這裡統一前置）
    dataset = dataset[dataset.core.isin(['OCCURRENCE', 'SAMPLINGEVENT'])]

    # source 過濾
    if source == 'GBIF':
        dataset = dataset[dataset.source == 'GBIF']
    elif source == 'not_GBIF':
        dataset = dataset[dataset.source != 'GBIF']

    # 排除夥伴單位
    if exclude_publishers:
        dataset = dataset[~dataset.publisherID.isin(exclude_publishers)]

    # 限定台灣發布者
    if only_tw_publishers:
        pub_resp = requests.get("https://portal.taibif.tw/api/v3/publisher?countryCode=TW")
        if pub_resp.status_code == 200:
            tw_ids = [p['publisherID'] for p in pub_resp.json()['data']]
            if exclude_publishers:
                tw_ids = [pid for pid in tw_ids if pid not in exclude_publishers]
            dataset = dataset[dataset.publisherID.isin(tw_ids)]

    # 排除重複資料集
    if exclude_datasets:
        dataset = dataset[~dataset.taibifDatasetID.isin(exclude_datasets)]
    if exclude_gbif_datasets:
        dataset = dataset[~dataset.gbifDatasetID.isin(exclude_gbif_datasets)]

    dataset = dataset.rename(columns={
        'publisherName': 'datasetPublisher',
        'license': 'datasetLicense',
    })

    dataset_list = dataset[['taibifDatasetID', 'numberOccurrence']].values.tolist()
    return dataset, dataset_list


def build_ds_name_basic(df, extra_cols=None):
    """
    模式 1, 2 用：直接從 df 取欄位 drop_duplicates, 不需 merge 外部 dataset。
    模式 1: 只有 datasetName + recordType (13 檔，最大宗)
    模式 2: 加 sourceDatasetID (1 檔)
    extra_cols: 額外要帶的欄位 list (預設 ['datasetName', 'recordType'])
    """
    cols = ['datasetName', 'recordType']
    if extra_cols:
        cols += extra_cols
    return df[cols].drop_duplicates().to_dict(orient='records')


def build_ds_name_with_merge(df, dataset, df_cols, dataset_cols, 
                             right_on, left_on='sourceDatasetID'):
                            #   left_on='sourceDatasetID', right_on='taibifDatasetID'):
    """
    模式 3, 4, 5 用：先從 df 挑欄位，再跟 dataset DataFrame merge license/publisher 等。
    模式 3: 要從 TaiBIF dataset 表 merge license/publisher(4 檔，少 recordType)
    模式 4: 模式 3 + recordType (1 檔, gbif 有帶 recordType 進去)
    模式 5: 從 TBN dataset 表 merge license (1 檔)

    df_cols: 從 df 取的欄位
    dataset_cols: 從 dataset 取的欄位（要包含 right_on）
    """
    ds_name = df[df_cols]
    ds_name = ds_name.merge(dataset[dataset_cols], left_on=left_on, right_on=right_on)
    return ds_name.drop_duplicates().to_dict(orient='records')

 


def update_dataset_key(ds_name, rights_holder, update_version, group, now):
    """
    UPSERT ds_name 到 dataset 表，回傳 tbiaDatasetID 對應表 (給呼叫端 merge 回 df)。

    比對優先順序:
        有 sourceDatasetID: (sourceDatasetID, rights_holder) → (name, rights_holder) → INSERT
        無 sourceDatasetID: (name, rights_holder) → INSERT

    註: (name, rights_holder) fallback 命中時會覆寫 sourceDatasetID 欄位，
        為了 (a) 補上之前沒有、現在才有的 sourceDatasetID
            (b) 將現在沒有的覆寫回空字串，以維持下次對應的一致性
        皆為有意設計。
    """
    # now = datetime.now() + timedelta(hours=8)

    with SessionLocal() as session:
        for r in ds_name:
            _upsert_dataset_row(session, r, rights_holder, update_version, group, now)
        session.commit()

        rows = session.execute(
            select(Dataset.tbiaDatasetID, Dataset.name, Dataset.sourceDatasetID)
            .where(Dataset.rights_holder == rights_holder)
            .where(Dataset.deprecated.is_(False))
        ).all()

    dataset_ids = pd.DataFrame(rows, columns=['tbiaDatasetID', 'datasetName', 'sourceDatasetID'])
    dataset_ids = dataset_ids.replace({None: '', np.nan: ''})
    dataset_ids = dataset_ids.merge(pd.DataFrame(ds_name))
    if len(dataset_ids):
        dataset_ids = dataset_ids[['tbiaDatasetID', 'datasetName', 'sourceDatasetID']].drop_duplicates()
    return dataset_ids


def _common_update_values(r, update_version, now):
    """UPDATE 與 ON CONFLICT DO UPDATE 共用欄位 (不含 match key 與 INSERT 專屬欄位)。"""
    return {
        'gbifDatasetID': r.get('gbifDatasetID'),
        'datasetURL': r.get('datasetURL'),
        'datasetLicense': r.get('datasetLicense') or 'OGDL',
        'datasetPublisher': r.get('datasetPublisher'),
        'deprecated': False,
        'modified': now,
        'update_version': update_version,
    }


def _upsert_dataset_row(session, r, rights_holder, update_version, group, now):
    """
    2-step SELECT → UPDATE/INSERT。
    INSERT 階段加 ON CONFLICT DO UPDATE 作為 race condition 的保險網。
    """
    name = r.get('datasetName')
    source_dataset_id = r.get('sourceDatasetID') or ''
    update_values = _common_update_values(r, update_version, now)

    existing = None
    matched_by = None

    # Step 1: 有 sourceDatasetID 時優先比 (sourceDatasetID, rights_holder)
    if source_dataset_id:
        existing = session.execute(
            select(Dataset).where(
                Dataset.sourceDatasetID == source_dataset_id,
                Dataset.rights_holder == rights_holder,
            )
        ).scalars().first()
        if existing is not None:
            matched_by = 'sourceDatasetID'

    # Step 2: fallback 比 (name, rights_holder)
    if existing is None:
        existing = session.execute(
            select(Dataset).where(
                Dataset.name == name,
                Dataset.rights_holder == rights_holder,
            )
        ).scalars().first()
        if existing is not None:
            matched_by = 'name'

    # 命中 → UPDATE (依命中路徑補另一個 key)
    if existing is not None:
        for k, v in update_values.items():
            setattr(existing, k, v)
        if matched_by == 'sourceDatasetID':
            existing.name = name
        else:  # matched_by == 'name'，含「無 sourceDatasetID 進來覆寫成空字串」情境
            existing.sourceDatasetID = source_dataset_id
        return

    # 沒命中 → INSERT (ON CONFLICT 防 race)
    insert_values = {
        'rights_holder': rights_holder,
        'name': name,
        'sourceDatasetID': source_dataset_id,
        'tbiaDatasetID': 'd' + str(bson.objectid.ObjectId()),
        'group': group,
        'created': now,
        **update_values,
    }
    stmt = pg_insert(Dataset).values(**insert_values).on_conflict_do_update(
        constraint='dataset_unique',
        set_=update_values,
    )
    session.execute(stmt)


def process_dataset(df, group, rights_holder, update_version, now, *,
                    extra_cols=None,
                    dataset=None,
                    df_cols=None,
                    dataset_cols=None,
                    left_on='sourceDatasetID',
                    right_on=None):
    """
    建立 ds_name → 註冊到 portal (取得 tbiaDatasetID) → merge 回 df。
    所有參數需明確傳入 (基本模式不用的欄位傳 None)。
 
    基本模式 (dataset=None，走 build_ds_name_basic):
        extra_cols: None 或 list (例: ['sourceDatasetID'])
        其他 merge 參數 (df_cols, dataset_cols, left_on, right_on) 傳 None
 
    Merge 模式 (dataset 為 DataFrame，走 build_ds_name_with_merge):
        必須傳入 df_cols / dataset_cols / left_on / right_on
        extra_cols 傳 None
    """
    if dataset is None:
        ds_name = build_ds_name_basic(df, extra_cols=extra_cols)
    else:
        ds_name = build_ds_name_with_merge(
            df, dataset,
            df_cols=df_cols,
            dataset_cols=dataset_cols,
            left_on=left_on,
            right_on=right_on,
        )
 
    return_dataset_id = update_dataset_key(
        ds_name=ds_name,
        rights_holder=rights_holder,
        update_version=update_version,
        group=group,
        now=now
    )
    return df.merge(return_dataset_id)


def update_dataset_deprecated(rights_holder, update_version):
    # update_version不等於這次的 改成 deprecated = 't'
    with SessionLocal() as session:
        session.execute(
            update(Dataset)
            .where(Dataset.rights_holder == rights_holder)
            .where(Dataset.update_version != update_version)
            .values(deprecated=True)
        )
        session.commit()
