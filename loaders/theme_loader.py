#!/usr/bin/env python3
"""
테마코드 데이터 로더
"""

import logging
import pandas as pd
from db_utils import get_connection, insert_dataframe

logger = logging.getLogger('theme_loader')

class ThemeLoader:
    """테마코드 데이터 로더"""
    
    def __init__(self):
        self.table_name = 'theme_code'
    
    def load_data(self, theme_parser):
        """테마코드 데이터 로드"""
        logger.info("테마코드 데이터 로드 시작")
        
        try:
            # 파서에서 데이터 가져오기
            data = theme_parser.get_data()
            
            if data is None or data.empty:
                logger.error("테마코드 데이터가 비어 있습니다.")
                return 0
            
            logger.info(f"테마코드 파싱 결과: {len(data)}개 레코드")
            
            # 데이터베이스에 적재
            try:
                success = insert_dataframe(self.table_name, data)
                if success:
                    logger.info(f"테마코드 데이터 로드 완료: {len(data)}개 레코드")
                    return len(data)
                else:
                    logger.error("테마코드 데이터 적재 실패")
                    return 0
            except Exception as e:
                logger.error(f"테마코드 데이터 적재 중 오류: {e}")
                return 0
                
        except Exception as e:
            logger.error(f"테마코드 데이터 로드 중 오류: {e}", exc_info=True)
            return 0
