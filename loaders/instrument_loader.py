import logging
import pandas as pd
from db_utils import insert_dataframe, count_records
from parsers.domestic_stock_parser import DomesticStockParser

logger = logging.getLogger('instrument_loader')

class InstrumentLoader:
    """종목 데이터 로더"""
    
    def __init__(self, file_mapping):
        self.file_mapping = file_mapping
        self.table_name = 'instruments'
    
    def load_domestic_stocks(self):
        """국내 주식 로드 (코스피, 코스닥, 코넥스)"""
        total_loaded = 0
        
        for market in ['코스피', '코스닥', '코넥스']:
            try:
                row = self.file_mapping[self.file_mapping['항목명'] == market].iloc[0]
                master_file = row['종목다운로드']
                header_file = row['헤더정보']
                
                logger.info(f"{market} 종목 데이터 로드 시작")
                parser = DomesticStockParser(master_file, header_file, market)
                data = parser.get_data()
                
                if data.empty:
                    logger.error(f"{market} 종목 데이터가 비어 있습니다.")
                    continue
                
                # 데이터베이스에 삽입
                rows = insert_dataframe(self.table_name, data)
                
                if rows > 0:
                    logger.info(f"{market} 종목 데이터 로드 완료: {rows}개")
                    total_loaded += rows
                else:
                    logger.warning(f"{market} 종목 데이터 적재 실패")
            except Exception as e:
                logger.error(f"{market} 종목 데이터 로드 중 오류: {e}", exc_info=True)
        
        return total_loaded
    
    def load_elw(self):
        """ELW 로드"""
        try:
            market = 'ELW'
            row = self.file_mapping[self.file_mapping['항목명'] == market].iloc[0]
            master_file = row['종목다운로드']
            header_file = row['헤더정보']
            
            logger.info(f"{market} 종목 데이터 로드 시작")
            # ELW 파서 클래스가 구현되어 있다면 사용, 아니면 DomesticStockParser로 대체
            try:
                from parsers.elw_parser import ELWParser
                parser = ELWParser(master_file, header_file, market)
            except ImportError:
                logger.warning("ELW 전용 파서가 없어 DomesticStockParser를 대체 사용합니다.")
                parser = DomesticStockParser(master_file, header_file, market)
            
            data = parser.get_data()
            
            if data.empty:
                logger.error(f"{market} 종목 데이터가 비어 있습니다.")
                return 0
            
            # 데이터베이스에 삽입
            rows = insert_dataframe(self.table_name, data)
            logger.info(f"{market} 종목 데이터 로드 완료: {rows}개")
            return rows
        except Exception as e:
            logger.error(f"ELW 데이터 로드 중 오류: {e}", exc_info=True)
            return 0
    
    def load_all(self):
        """모든 종목 데이터 로드"""
        logger.info("종목 데이터 로드 시작")
        
        total_loaded = 0
        
        # 1. 국내 주식 (코스피, 코스닥, 코넥스)
        stock_count = self.load_domestic_stocks()
        total_loaded += stock_count
        logger.info(f"국내 주식 총 {stock_count}개 로드 완료")
        
        # 2. ELW
        elw_count = self.load_elw()
        total_loaded += elw_count
        logger.info(f"ELW 총 {elw_count}개 로드 완료")
        
        # 여기에 다른 종목 유형 로드 메서드 추가 가능
        # self.load_futures()
        # self.load_options()
        # 등등...
        
        # 최종 확인
        final_count = count_records(self.table_name)
        logger.info(f"종목 데이터 로드 완료: 총 {final_count}개 레코드 (적재 시도: {total_loaded}개)")
        
        return final_count > 0