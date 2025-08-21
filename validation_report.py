#!/usr/bin/env python3
"""
KIS 데이터 검증 및 마크다운 리포트 생성
"""

import logging
import pymysql
import json
import os
from datetime import datetime
from config import DB_CONFIG

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/kis_validation.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('validation_report')

class ValidationReporter:
    """검증 결과를 마크다운으로 생성하는 클래스"""
    
    def __init__(self):
        self.conn = pymysql.connect(**DB_CONFIG)
        self.report_data = {
            'timestamp': datetime.now(),
            'tables': {},
            'validations': [],
            'status': 'UNKNOWN'
        }
    
    def run_validation(self):
        """전체 검증 실행"""
        logger.info("KIS 데이터 검증 시작")
        
        try:
            # 1. 테이블 통계 수집
            self._collect_table_stats()
            
            # 2. 기본 검증 실행
            self._run_basic_validations()
            
            # 3. 비즈니스 검증 실행
            self._run_business_validations()
            
            # 4. 전체 상태 결정
            self._determine_overall_status()
            
            # 5. 마크다운 리포트 생성
            self._generate_markdown_report()
            
            logger.info("검증 완료")
            return True
            
        except Exception as e:
            logger.error(f"검증 중 오류: {e}", exc_info=True)
            return False
        finally:
            self.conn.close()
    
    def _collect_table_stats(self):
        """테이블 통계 수집"""
        cursor = self.conn.cursor()
        
        tables = ['instruments', 'sector_code', 'theme_code', 'member_code']
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            self.report_data['tables'][table] = {'count': count}
            
        # instruments 세부 통계
        cursor.execute("SELECT market_type, COUNT(*) FROM instruments GROUP BY market_type")
        market_stats = {}
        for row in cursor.fetchall():
            market_stats[row[0]] = row[1]
        self.report_data['tables']['instruments']['by_market'] = market_stats
    
    def _run_basic_validations(self):
        """기본 검증 실행"""
        cursor = self.conn.cursor()
        
        validations = [
            {
                'name': '삼성전자 존재 검증',
                'query': "SELECT COUNT(*) FROM instruments WHERE instrument_name = '삼성전자'",
                'expected_min': 1,
                'type': 'count'
            },
            {
                'name': '코스피 종목 수 검증',
                'query': "SELECT COUNT(*) FROM instruments WHERE market_type = '코스피'",
                'expected_min': 2000,
                'type': 'count'
            },
            {
                'name': '코스닥 종목 수 검증', 
                'query': "SELECT COUNT(*) FROM instruments WHERE market_type = '코스닥'",
                'expected_min': 1500,
                'type': 'count'
            },
            {
                'name': '업종코드 존재 검증',
                'query': "SELECT COUNT(*) FROM sector_code",
                'expected_min': 400,
                'type': 'count'
            },
            {
                'name': '테마코드 존재 검증',
                'query': "SELECT COUNT(*) FROM theme_code", 
                'expected_min': 200,
                'type': 'count'
            }
        ]
        
        for validation in validations:
            try:
                cursor.execute(validation['query'])
                actual = cursor.fetchone()[0]
                
                if validation['type'] == 'count':
                    passed = actual >= validation['expected_min']
                    
                self.report_data['validations'].append({
                    'name': validation['name'],
                    'status': 'PASS' if passed else 'FAIL',
                    'expected': f">= {validation['expected_min']}",
                    'actual': actual,
                    'message': f"예상: {validation['expected_min']}개 이상, 실제: {actual}개"
                })
                
            except Exception as e:
                self.report_data['validations'].append({
                    'name': validation['name'],
                    'status': 'ERROR',
                    'message': f"검증 오류: {e}"
                })
    
    def _run_business_validations(self):
        """비즈니스 검증 실행"""
        cursor = self.conn.cursor()
        
        # 삼성전자 정확성 검증
        cursor.execute("""
            SELECT short_code, instrument_code, instrument_name 
            FROM instruments 
            WHERE instrument_name = '삼성전자'
        """)
        samsung = cursor.fetchone()
        
        if samsung:
            expected_short = '005930'
            expected_standard = 'KR7005930003'
            
            samsung_valid = (samsung[0] == expected_short and 
                           samsung[1] == expected_standard)
            
            self.report_data['validations'].append({
                'name': '삼성전자 코드 정확성 검증',
                'status': 'PASS' if samsung_valid else 'FAIL',
                'expected': f"단축코드: {expected_short}, 표준코드: {expected_standard}",
                'actual': f"단축코드: {samsung[0]}, 표준코드: {samsung[1]}",
                'message': "삼성전자 코드 매핑이 정확합니다" if samsung_valid else "삼성전자 코드 매핑 오류"
            })
        
        # NULL 값 검증
        cursor.execute("""
            SELECT COUNT(*) FROM instruments 
            WHERE short_code IS NULL OR instrument_code IS NULL OR instrument_name IS NULL
        """)
        null_count = cursor.fetchone()[0]
        
        self.report_data['validations'].append({
            'name': '필수 필드 NULL 검증',
            'status': 'PASS' if null_count == 0 else 'FAIL',
            'expected': "0개",
            'actual': f"{null_count}개",
            'message': "필수 필드에 NULL 값이 없습니다" if null_count == 0 else f"{null_count}개의 NULL 값 발견"
        })
    
    def _determine_overall_status(self):
        """전체 상태 결정"""
        passed = sum(1 for v in self.report_data['validations'] if v['status'] == 'PASS')
        failed = sum(1 for v in self.report_data['validations'] if v['status'] == 'FAIL')
        errors = sum(1 for v in self.report_data['validations'] if v['status'] == 'ERROR')
        
        if errors > 0:
            self.report_data['status'] = 'ERROR'
        elif failed > 0:
            self.report_data['status'] = 'FAIL'
        else:
            self.report_data['status'] = 'PASS'
            
        self.report_data['summary'] = {
            'total': len(self.report_data['validations']),
            'passed': passed,
            'failed': failed, 
            'errors': errors,
            'success_rate': round(passed / len(self.report_data['validations']) * 100, 1)
        }
    
    def _generate_markdown_report(self):
        """마크다운 리포트 생성"""
        timestamp = self.report_data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        
        md_content = f"""# KIS 데이터 검증 리포트

## 📊 검증 개요
- **검증 시간**: {timestamp}
- **전체 상태**: {'✅ PASS' if self.report_data['status'] == 'PASS' else '❌ FAIL' if self.report_data['status'] == 'FAIL' else '⚠️ ERROR'}
- **성공률**: {self.report_data['summary']['success_rate']}% ({self.report_data['summary']['passed']}/{self.report_data['summary']['total']})

## 📈 테이블 통계

### 전체 테이블
| 테이블 | 레코드 수 |
|--------|-----------|
"""
        
        for table, stats in self.report_data['tables'].items():
            md_content += f"| {table} | {stats['count']:,}개 |\n"
        
        # 시장별 분포
        if 'by_market' in self.report_data['tables']['instruments']:
            md_content += "\n### 시장별 분포\n| 시장 | 종목 수 |\n|------|--------|\n"
            for market, count in self.report_data['tables']['instruments']['by_market'].items():
                md_content += f"| {market} | {count:,}개 |\n"
        
        # 검증 결과
        md_content += "\n## 🔍 검증 결과\n\n"
        
        for validation in self.report_data['validations']:
            status_icon = "✅" if validation['status'] == 'PASS' else "❌" if validation['status'] == 'FAIL' else "⚠️"
            md_content += f"### {status_icon} {validation['name']}\n"
            md_content += f"- **상태**: {validation['status']}\n"
            
            if 'expected' in validation:
                md_content += f"- **예상**: {validation['expected']}\n"
                md_content += f"- **실제**: {validation['actual']}\n"
            
            md_content += f"- **메시지**: {validation['message']}\n\n"
        
        # 요약
        md_content += f"""## 📋 요약
- **총 검증 항목**: {self.report_data['summary']['total']}개
- **통과**: {self.report_data['summary']['passed']}개
- **실패**: {self.report_data['summary']['failed']}개  
- **오류**: {self.report_data['summary']['errors']}개
- **성공률**: {self.report_data['summary']['success_rate']}%

---
*리포트 생성 시간: {timestamp}*
"""
        
        # 파일 저장
        report_filename = f"reports/kis_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write(md_content)
        
        logger.info(f"📄 마크다운 리포트가 {report_filename}에 저장되었습니다")
        
        # JSON 리포트도 저장
        json_filename = f"reports/kis_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(self.report_data, f, ensure_ascii=False, indent=2, default=str)
        
        return report_filename

def main():
    """메인 실행 함수"""
    reporter = ValidationReporter()
    success = reporter.run_validation()
    return success

if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
