import pymysql
import pandas as pd
from config import DB_CONFIG, BATCH_SIZE
import logging

logger = logging.getLogger('db_utils')

def get_connection():
    """데이터베이스 연결을 반환합니다."""
    return pymysql.connect(
        host=DB_CONFIG['host'],
        port=DB_CONFIG['port'],
        user=DB_CONFIG['user'],
        password=DB_CONFIG['password'],
        database=DB_CONFIG['database'],
        charset=DB_CONFIG['charset'],
        autocommit=False
    )

def execute_query(query, params=None, fetch=False, many=False):
    """SQL 쿼리를 실행합니다."""
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            if many:
                cursor.executemany(query, params)
            else:
                cursor.execute(query, params)
            if fetch:
                return cursor.fetchall()
            conn.commit()
            return cursor.rowcount
    except Exception as e:
        conn.rollback()
        logger.error(f"쿼리 실행 오류: {e}")
        raise
    finally:
        conn.close()

def insert_dataframe(table_name, df, batch_size=BATCH_SIZE):
    """DataFrame을 데이터베이스 테이블에 삽입합니다."""
    if df.empty:
        logger.warning(f"{table_name} 테이블에 삽입할 데이터가 없습니다.")
        return 0
    
    # DataFrame 컬럼을 테이블 컬럼에 맞게 조정
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # 테이블 컬럼 정보 가져오기
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            columns = [column[0] for column in cursor.fetchall()]
            
            # DataFrame에 있는 컬럼만 사용 (created_at, updated_at 제외)
            exclude_columns = ['created_at', 'updated_at']
            valid_columns = [col for col in columns if col in df.columns and col not in exclude_columns]
            
            if not valid_columns:
                logger.error(f"{table_name} 테이블에 삽입할 유효한 컬럼이 없습니다.")
                return 0
            
            placeholders = ', '.join(['%s'] * len(valid_columns))
            columns_str = ', '.join(valid_columns)
            
            # 데이터 삽입 쿼리 (IGNORE 사용하여 중복 처리)
            query = f"INSERT IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            
            # 배치 처리
            total_rows = 0
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i+batch_size]
                values = []
                for _, row in batch_df.iterrows():
                    value_tuple = tuple(
                        row[col] if col in row.index and pd.notna(row[col]) else None 
                        for col in valid_columns
                    )
                    values.append(value_tuple)
                
                cursor.executemany(query, values)
                total_rows += len(values)
                logger.info(f"{table_name}: {i+len(values)}/{len(df)} 행 삽입 완료")
            
            conn.commit()
            logger.info(f"{table_name} 테이블에 총 {total_rows} 행이 삽입되었습니다.")
            return total_rows
    except Exception as e:
        conn.rollback()
        logger.error(f"{table_name} 테이블 데이터 삽입 오류: {e}")
        raise
    finally:
        conn.close()

def count_records(table_name):
    """테이블의 레코드 수를 반환합니다."""
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            return count
    finally:
        conn.close()

def get_file_mapping():
    """kis_file_sets.csv 파일에서 파일 매핑 정보를 가져옵니다."""
    from config import FILE_SETS_CSV
    return pd.read_csv(FILE_SETS_CSV, encoding='utf-8')

def truncate_tables():
    """모든 테이블을 초기화합니다."""
    tables = [
        'instrument_theme_map',  # 외래 키 제약으로 먼저 삭제
        'instruments',
        'member_code',
        'sector_code',
        'theme_code'
    ]
    
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            # 외래키 제약 임시 비활성화
            cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
            
            for table in tables:
                logger.info(f"테이블 초기화: {table}")
                cursor.execute(f"TRUNCATE TABLE {table}")
            
            # 외래키 제약 다시 활성화
            cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        
        conn.commit()
        logger.info("모든 테이블 초기화 완료")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"테이블 초기화 오류: {e}")
        return False
    finally:
        conn.close()

def clear_table(table_name):
    """테이블의 모든 데이터를 삭제합니다."""
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DELETE FROM {table_name}")
            conn.commit()
            logger.info(f"{table_name} 테이블 데이터가 삭제되었습니다.")
    except Exception as e:
        conn.rollback()
        logger.error(f"{table_name} 테이블 데이터 삭제 오류: {e}")
        raise
    finally:
        conn.close()
