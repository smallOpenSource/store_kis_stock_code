import logging
import pandas as pd
from db_utils import insert_dataframe, count_records
from parsers.member_parser import MemberParser

logger = logging.getLogger('member_loader')

class MemberLoader:
    """회원사 코드 로더"""
    
    def __init__(self, file_mapping):
        self.file_mapping = file_mapping
        self.table_name = 'member_code'
    
    def load_all(self):
        """회원사 코드 데이터 로드"""
        try:
            # 회원사코드 파일 찾기
            row = self.file_mapping[self.file_mapping['항목명'] == '회원사코드'].iloc[0]
            master_file = row['종목다운로드']
            header_file = row['헤더정보']
            
            logger.info("회원사코드 데이터 로드 시작")
            parser = MemberParser(master_file, header_file)
            data = parser.get_data()
            
            if data.empty:
                logger.error("회원사코드 데이터가 비어있습니다.")
                return False
            
            # 데이터베이스에 삽입
            rows = insert_dataframe(self.table_name, data)
            success = rows > 0
            
            if success:
                logger.info(f"회원사코드 데이터 로드 완료: {rows}개 레코드")
            else:
                logger.error("회원사코드 데이터 로드 실패")
            
            return success
        except Exception as e:
            logger.error(f"회원사코드 데이터 로드 중 오류: {e}", exc_info=True)
            return False
