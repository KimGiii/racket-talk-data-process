import json
import logging
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DB 설정
DATABASE_URL = "postgresql+psycopg2://admin123:admin123!@racket-talk-db.c348isg6kpwg.ap-southeast-2.rds.amazonaws.com:5432/racket_talk_db"
engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
Base = declarative_base()

# ORM 모델 정의: courts 테이블
class Court(Base):
    __tablename__ = "courts"
    court_id = Column(Integer, primary_key=True, autoincrement=True)
    court_name = Column(String(50), nullable=False)

# ORM 모델 정의: courts_detail 테이블
class CourtDetail(Base):
    __tablename__ = "courts_detail"
    court_detail_id = Column(String(200), primary_key=True)  # 고유 식별자 (UUID 등 사용 가능)
    court_id = Column(Integer, ForeignKey("courts.court_id"), nullable=False)  # courts 테이블 참조
    detail_court_name = Column(String(50), nullable=False)
    detail_operating_time = Column(String(50), nullable=True)
    target_user_info = Column(String(50), nullable=True)

# 데이터를 로드하고 courts 테이블의 court_id를 참조하여 courts_detail 데이터 삽입
def load_and_insert_details(json_path: str):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    records = data.get("DATA", [])
    session = SessionLocal()

    try:
        for record in records:
            # courts 테이블에서 court_name으로 court_id 가져오기
            court_name = record.get("court_name", "").strip()
            court = session.query(Court).filter(Court.court_name == court_name).first()

            if not court:
                logger.warning(f"[WARNING] Court not found for court_name: {court_name}")
                continue  # 해당 court_name이 없으면 건너뜀
            
            # courts_detail 데이터 삽입
            court_detail = CourtDetail(
                court_detail_id=record.get("court_detail_id", f"default_{court.court_id}"),  # 기본값으로 고유ID 생성
                court_id=court.court_id,
                detail_court_name=record.get("detail_court_name", court_name),
                detail_operating_time=record.get("detail_operating_time", ""),
                target_user_info=record.get("target_user_info", "")
            )
            session.add(court_detail)

        session.commit()
        logger.info(f"[SUCCESS] courts_detail 테이블에 {len(records)}개의 데이터가 삽입되었습니다.")
    except Exception as e:
        session.rollback()
        logger.error(f"[ERROR] courts_detail 데이터 삽입 중 오류 발생: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    # 테이블 생성 (존재하지 않을 경우)
    Base.metadata.create_all(bind=engine)

    # JSON 파일 경로
    json_file_path = "../processed_data/테니스장_상세.json"

    # 데이터 로드 및 삽입 실행
    load_and_insert_details(json_file_path)