import pandas as pd
import logging
from .base_parser import BaseParser

logger = logging.getLogger('theme_parser')

class ThemeParser(BaseParser):
    """테마코드 마스터 파일 파서"""
    
    def __init__(self, master_file, header_file=None):
        super().__init__(master_file, header_file, '테마코드')
    
    def parse(self):
        """테마코드 마스터 파일 파싱"""
        extracted_file = self.extract_if_zip()
        
        result = []
        record_count = 0
        
        try:
            with open(extracted_file, mode="r", encoding="cp949") as f:
                for row in f:
                    record_count += 1
                    
                    try:
                        # 샘플 코드 기반 파싱
                        theme_code = row[0:3].strip()  # 테마코드
                        theme_name = row[3:-10].rstrip()  # 테마명
                        stock_code = row[-10:].rstrip()  # 종목코드
                        
                        # 유효성 검증
                        if not theme_code or len(theme_code) == 0:
                            continue
                            
                        if not theme_name or theme_name.strip() == '':
                            continue
                        
                        record = {
                            'theme_code': theme_code,
                            'theme_name': theme_name.strip(),
                            'stock_code': stock_code.strip() if stock_code else None,
                            'is_active': 'Y',
                            'created_at': pd.Timestamp.now(),
                            'updated_at': pd.Timestamp.now()
                        }
                        result.append(record)
                        
                    except Exception as e:
                        logger.warning(f"레코드 {record_count} 파싱 오류: {e}")
                        continue
        
        except Exception as e:
            logger.error(f"테마코드 파일 읽기 오류: {e}")
            raise
        
        logger.info(f"테마코드 마스터 파일 파싱 완료: {len(result)}개 레코드")
        return result
    
    def transform(self, parsed_data):
        """데이터 변환 및 정제"""
        if not parsed_data:
            logger.warning("테마코드 변환할 데이터 없음")
            return pd.DataFrame()
            
        df = pd.DataFrame(parsed_data)
        
        # 중복 제거 (테마코드별로)
        df = df.drop_duplicates(subset=['theme_code', 'stock_code'])
        
        # 데이터 타입 정리
        df['theme_code'] = df['theme_code'].astype(str)
        df['theme_name'] = df['theme_name'].astype(str)
        
        logger.info(f"테마코드 변환 완료: {len(df)}개 유효 레코드")
        
        return df
