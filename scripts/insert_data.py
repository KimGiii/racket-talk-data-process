import json
import os
from decimal import Decimal, InvalidOperation
import logging
from sqlalchemy import create_engine, Column, Integer, String, Numeric
from sqlalchemy.orm import declarative_base, sessionmaker

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLAlchemy DB 설정
DATABASE_URL = "postgresql+psycopg2://admin123:admin123!@racket-talk-db.c348isg6kpwg.ap-southeast-2.rds.amazonaws.com:5432/racket_talk_db"
engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()


# ORM 모델 정의
class Court(Base):
    __tablename__ = "courts"
    court_id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    court_name = Column(String(50), nullable=False)
    address = Column(String(100), nullable=True)
    telno = Column(String(50), nullable=True)
    court_image = Column(String(500), nullable=True)
    lng = Column(Numeric(12, 8), nullable=False)  # 소수점 8자리까지 저장
    lat = Column(Numeric(12, 8), nullable=False)  # 소수점 8자리까지 저장


# JSON 위도 경도 데이터 검증 함수
def validate_decimal(value, default=0.0):
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError, TypeError):
        logger.warning(f"Invalid value for Decimal conversion: {value}. Using default: {default}")
        return Decimal(default)


# JSON 데이터를 로드하고 삽입하는 함수
def load_and_insert(json_path: str):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = data.get("DATA", [])
    session = SessionLocal()

    try:
        for record in records:
            # address 키의 다양한 오타를 고려 ("address", "addresss", "addres")
            address = record.get("address") or record.get("addresss") or record.get("addres")
            
            # lng, lat 값을 검증 및 변환
            lng_data = validate_decimal(record.get("lng"))
            lat_data = validate_decimal(record.get("lat"))
            
            # 데이터 삽입
            court = Court(
                court_name=record.get("court_name"),
                address=address,
                telno=record.get("telno"),
                court_image=record.get("court_image"),
                lng=lng_data,
                lat=lat_data,
            )
            session.add(court)

        session.commit()  # 변경사항 커밋
        logger.info(f"[SUCCESS] {len(records)}개의 기록이 성공적으로 삽입되었습니다.")
    except Exception as e:
        session.rollback()  # 롤백
        logger.error(f"[ERROR] 데이터 삽입 중 오류 발생: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    # 테이블 생성 (존재하지 않을 경우)
    Base.metadata.create_all(bind=engine)

    # JSON 파일 경로
    json_file_path = "../processed_data/테니스장_팝업.json"

    # 로드 및 저장
    load_and_insert(json_file_path)