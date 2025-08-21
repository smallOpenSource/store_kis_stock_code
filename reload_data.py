import logging
import pandas as pd
import os
from config import FILE_SETS_CSV
from db_utils import truncate_tables
from loaders.member_loader import MemberLoader
from loaders.instrument_loader import InstrumentLoader
from parsers.domestic_stock_parser import DomesticStockParser
from parsers.member_parser import MemberParser

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='logs/kis_reload.log',
    filemode='w'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
logger = logging.getLogger('reload_main')

def main():
    """KIS 종목정보 재적재 실행"""
    try:
        logger.info("\n" + "="*60)
        logger.info("KIS 종목정보 데이터 재적재 시작")
        logger.info("="*60)
        
        # 1. 파일 매핑 정보 로드
        file_mapping = pd.read_csv(FILE_SETS_CSV, encoding='utf-8')
        logger.info(f"파일 매핑 정보 로드 완료: {len(file_mapping)}개 항목")
        
        # 2. 테이블 초기화
        logger.info("모든 테이블 초기화 시작")
        if not truncate_tables():
            logger.error("테이블 초기화 실패")
            return False
        logger.info("모든 테이블 초기화 완료")
        
        # 3. 참조 테이블 적재
        logger.info("참조 테이블 적재 시작")
        # 회원사 코드 적재
        member_loader = MemberLoader(file_mapping)
        member_result = member_loader.load_all()
        logger.info(f"회원사 코드 적재 {'성공' if member_result else '실패'}")
        
        # 업종코드 적재
        try:
            from parsers.sector_parser import SectorParser
            from loaders.sector_loader import SectorLoader
            
            sector_parser = SectorParser(
                master_file='업종코드_마스터_idxcode.mst.zip',
                header_file='업종코드_헤더_업종코드정보.h'
            )
            sector_loader = SectorLoader()
            sector_count = sector_loader.load_data(sector_parser)
            sector_result = sector_count > 0
            logger.info(f"업종코드 적재 {'성공' if sector_result else '실패'}: {sector_count}개")
        except Exception as e:
            logger.error(f"업종코드 적재 오류: {e}")
            sector_result = False
        
        # 테마코드 적재
        try:
            from parsers.theme_parser import ThemeParser
            from loaders.theme_loader import ThemeLoader
            
            theme_parser = ThemeParser(
                master_file='테마코드_마스터_theme_code.mst.zip',
                header_file='테마코드_헤더_테마코드정보.h'
            )
            theme_loader = ThemeLoader()
            theme_count = theme_loader.load_data(theme_parser)
            theme_result = theme_count > 0
            logger.info(f"테마코드 적재 {'성공' if theme_result else '실패'}: {theme_count}개")
        except Exception as e:
            logger.error(f"테마코드 적재 오류: {e}")
            theme_result = False
        
        # 4. 종목 테이블 적재
        logger.info("종목 테이블 적재 시작")
        instrument_loader = InstrumentLoader(file_mapping)
        instrument_result = instrument_loader.load_all()
        logger.info(f"종목 테이블 적재 {'성공' if instrument_result else '실패'}")
        
        # 5. 결과 요약
        overall_success = member_result and instrument_result
        logger.info("\n" + "="*60)
        logger.info(f"KIS 종목정보 데이터 재적재 {'성공' if overall_success else '일부 실패'}")
        logger.info("="*60)
        
        return overall_success
    except Exception as e:
        logger.error(f"데이터 재적재 중 오류 발생: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
