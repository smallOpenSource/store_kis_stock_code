import pandas as pd
import logging
from .base_parser import BaseParser

logger = logging.getLogger('sector_parser')

class SectorParser(BaseParser):
    """업종코드 마스터 파일 파서"""
    
    def __init__(self, master_file, header_file=None):
        super().__init__(master_file, header_file, '업종코드')
    
    def parse(self):
        """업종코드 마스터 파일 파싱"""
        extracted_file = self.extract_if_zip()
        
        result = []
        record_count = 0
        
        try:
            with open(extracted_file, mode="r", encoding="cp949") as f:
                for row in f:
                    record_count += 1
                    
                    try:
                        # 샘플 코드 기반 파싱
                        sector_code = row[1:5].strip()  # 업종코드 4자리 (맨 앞 1자리 제거)
                        sector_name = row[3:43].rstrip()  # 업종명
                        
                        # 유효성 검증
                        if not sector_code or len(sector_code) == 0:
                            continue
                            
                        if not sector_name or sector_name.strip() == '':
                            continue
                        
                        record = {
                            'sector_code': sector_code,
                            'sector_name': sector_name.strip(),
                            'sector_level': 1,  # 기본값
                            'parent_sector_code': None,  # 기본값
                            'is_active': 'Y',
                            'created_at': pd.Timestamp.now(),
                            'updated_at': pd.Timestamp.now()
                        }
                        result.append(record)
                        
                    except Exception as e:
                        logger.warning(f"레코드 {record_count} 파싱 오류: {e}")
                        continue
        
        except Exception as e:
            logger.error(f"업종코드 파일 읽기 오류: {e}")
            raise
        
        logger.info(f"업종코드 마스터 파일 파싱 완료: {len(result)}개 레코드")
        return result
    
    def transform(self, parsed_data):
        """데이터 변환 및 정제"""
        if not parsed_data:
            logger.warning("업종코드 변환할 데이터 없음")
            return pd.DataFrame()
            
        df = pd.DataFrame(parsed_data)
        
        # 중복 제거
        df = df.drop_duplicates(subset=['sector_code'])
        
        # 데이터 타입 정리
        df['sector_code'] = df['sector_code'].astype(str)
        df['sector_name'] = df['sector_name'].astype(str)
        
        logger.info(f"업종코드 변환 완료: {len(df)}개 유효 레코드")
        
        return df
