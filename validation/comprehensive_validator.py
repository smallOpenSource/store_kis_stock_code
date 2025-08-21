import logging
import pandas as pd
from datetime import datetime
import sys
import os

# 상위 디렉토리를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from .file_analyzer import FileAnalyzer
from .db_validator import DatabaseValidator
from .business_validator import BusinessValidator
from .sample_validator import SampleValidator

logger = logging.getLogger('comprehensive_validator')

class ComprehensiveValidator:
    """통합 검증 실행기"""
    
    def __init__(self):
        self.file_analyzer = FileAnalyzer()
        self.db_validator = DatabaseValidator()
        self.business_validator = BusinessValidator()
        self.sample_validator = SampleValidator()
        self.validation_report = {}
    
    def run_full_validation(self, file_mapping):
        """전체 검증 실행"""
        logger.info("=" * 60)
        logger.info("KIS 데이터 통합 검증 시작")
        logger.info("=" * 60)
        start_time = datetime.now()
        
        try:
            # 1. 파일 분석
            logger.info("1️⃣  원시 파일 분석 중...")
            file_stats = self.file_analyzer.analyze_all_files(file_mapping)
            
            # 2. 데이터베이스 기본 검증
            logger.info("2️⃣  데이터베이스 기본 검증 중...")
            db_stats = self.db_validator.count_table_records()
            data_type_checks = self.db_validator.check_data_types()
            integrity_checks = self.db_validator.check_referential_integrity()
            
            # 3. 비즈니스 로직 검증
            logger.info("3️⃣  비즈니스 로직 검증 중...")
            market_validation, market_distribution = self.business_validator.validate_market_distribution()
            code_format_checks = self.business_validator.validate_code_formats()
            alias_stats = self.business_validator.validate_alias_quality()
            completeness_checks = self.business_validator.validate_data_completeness()
            
            # 4. 샘플 데이터 검증
            logger.info("4️⃣  샘플 데이터 검증 중...")
            known_stock_checks = self.sample_validator.validate_known_instruments()
            search_checks = self.sample_validator.validate_search_functionality()
            etf_validation = self.sample_validator.validate_etf_samples()
            market_coverage = self.sample_validator.validate_market_coverage()
            
            # 5. 파일 vs DB 레코드 수 비교
            logger.info("5️⃣  파일-DB 레코드 수 비교 중...")
            count_comparison = self._compare_file_db_counts(file_stats, db_stats)
            
            # 검증 결과 종합
            end_time = datetime.now()
            
            self.validation_report = {
                'metadata': {
                    'timestamp': end_time.isoformat(),
                    'duration': str(end_time - start_time),
                    'validation_version': '1.0'
                },
                'file_analysis': {
                    'file_stats': file_stats,
                    'total_files_analyzed': len(file_stats)
                },
                'database_validation': {
                    'table_stats': db_stats,
                    'data_type_checks': data_type_checks,
                    'integrity_checks': integrity_checks
                },
                'business_validation': {
                    'market_validation': market_validation,
                    'market_distribution': market_distribution,
                    'code_format_checks': code_format_checks,
                    'completeness_checks': completeness_checks,
                    'alias_stats': alias_stats
                },
                'sample_validation': {
                    'known_stock_checks': known_stock_checks,
                    'search_checks': search_checks,
                    'etf_validation': etf_validation,
                    'market_coverage': market_coverage
                },
                'count_comparison': count_comparison,
                'overall_result': self._calculate_overall_result()
            }
            
            # 검증 리포트 출력
            self._print_validation_report()
            
            return self.validation_report
            
        except Exception as e:
            logger.error(f"검증 실행 중 오류 발생: {e}", exc_info=True)
            raise
    
    def _compare_file_db_counts(self, file_stats, db_stats):
        """파일과 DB 레코드 수 비교"""
        comparison = {}
        
        # 종목별 매핑
        file_to_db_mapping = {
            '코스피': 'instruments',
            '코스닥': 'instruments', 
            '코넥스': 'instruments',
            'ELW': 'instruments',
            '회원사코드': 'member_code',
            '업종코드': 'sector_code',
            '테마코드': 'theme_code'
        }
        
        instruments_total_expected = 0
        instruments_breakdown = {}
        
        for item_name, table_name in file_to_db_mapping.items():
            if item_name in file_stats and file_stats[item_name]['record_count'] is not None:
                file_count = file_stats[item_name]['record_count']
                
                if table_name == 'instruments':
                    instruments_total_expected += file_count
                    instruments_breakdown[item_name] = file_count
                else:
                    db_count = db_stats.get(table_name, 0)
                    
                    # 허용 오차 범위 (10% 또는 최소 5개)
                    tolerance = max(5, int(file_count * 0.1))
                    is_match = abs(file_count - db_count) <= tolerance
                    
                    comparison[item_name] = {
                        'file_count': file_count,
                        'db_count': db_count,
                        'difference': db_count - file_count,
                        'tolerance': tolerance,
                        'is_match': is_match,
                        'match_percentage': round((min(file_count, db_count) / max(file_count, db_count)) * 100, 2) if max(file_count, db_count) > 0 else 0
                    }
                    
                    if is_match:
                        logger.info(f"✓ {item_name}: 파일 {file_count:,} vs DB {db_count:,} (차이: {db_count - file_count:+,})")
                    else:
                        logger.warning(f"✗ {item_name}: 파일 {file_count:,} vs DB {db_count:,} (차이: {db_count - file_count:+,}, 허용범위: ±{tolerance})")
        
        # instruments 테이블 전체 비교
        instruments_db_count = db_stats.get('instruments', 0)
        instruments_tolerance = max(50, int(instruments_total_expected * 0.1))
        instruments_match = abs(instruments_total_expected - instruments_db_count) <= instruments_tolerance
        
        comparison['instruments_total'] = {
            'file_count': instruments_total_expected,
            'db_count': instruments_db_count,
            'difference': instruments_db_count - instruments_total_expected,
            'tolerance': instruments_tolerance,
            'is_match': instruments_match,
            'breakdown': instruments_breakdown,
            'match_percentage': round((min(instruments_total_expected, instruments_db_count) / max(instruments_total_expected, instruments_db_count)) * 100, 2) if max(instruments_total_expected, instruments_db_count) > 0 else 0
        }
        
        if instruments_match:
            logger.info(f"✓ 전체 종목: 파일 {instruments_total_expected:,} vs DB {instruments_db_count:,} (일치율: {comparison['instruments_total']['match_percentage']}%)")
        else:
            logger.warning(f"✗ 전체 종목: 파일 {instruments_total_expected:,} vs DB {instruments_db_count:,} (차이: {instruments_db_count - instruments_total_expected:+,})")
        
        return comparison
    
    def _calculate_overall_result(self):
        """전체 검증 결과 계산"""
        total_checks = 0
        passed_checks = 0
        critical_failures = 0
        
        # 각 검증 항목의 성공/실패 카운트
        check_categories = [
            ('database_validation.data_type_checks', True),  # 중요한 검사
            ('database_validation.integrity_checks', False),
            ('business_validation.market_validation', True),  # 중요한 검사
            ('business_validation.code_format_checks', True),  # 중요한 검사
            ('business_validation.completeness_checks', False),
            ('sample_validation.known_stock_checks', True),  # 중요한 검사
            ('sample_validation.search_checks', False),
        ]
        
        for category_path, is_critical in check_categories:
            checks = self._get_nested_value(self.validation_report, category_path)
            if checks and isinstance(checks, list):
                for check in checks:
                    total_checks += 1
                    if check.get('result', False):
                        passed_checks += 1
                    elif is_critical:
                        critical_failures += 1
        
        # 파일-DB 비교 결과 추가 (중요한 검사)
        if 'count_comparison' in self.validation_report:
            for item, comparison in self.validation_report['count_comparison'].items():
                total_checks += 1
                if comparison.get('is_match', False):
                    passed_checks += 1
                else:
                    critical_failures += 1
        
        # ETF 및 시장 커버리지 검증 추가
        etf_validation = self._get_nested_value(self.validation_report, 'sample_validation.etf_validation')
        market_coverage = self._get_nested_value(self.validation_report, 'sample_validation.market_coverage')
        
        for validation in [etf_validation, market_coverage]:
            if validation:
                total_checks += 1
                if validation.get('result', False):
                    passed_checks += 1
        
        success_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0
        
        # 전체 상태 결정
        if critical_failures > 0:
            overall_status = 'CRITICAL_FAIL'
        elif success_rate >= 95:
            overall_status = 'EXCELLENT'
        elif success_rate >= 90:
            overall_status = 'GOOD'
        elif success_rate >= 80:
            overall_status = 'ACCEPTABLE'
        else:
            overall_status = 'FAIL'
        
        return {
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'failed_checks': total_checks - passed_checks,
            'critical_failures': critical_failures,
            'success_rate': round(success_rate, 2),
            'overall_status': overall_status
        }
    
    def _get_nested_value(self, data, path):
        """중첩된 딕셔너리에서 값 가져오기"""
        keys = path.split('.')
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return None
        return data
    
    def _print_validation_report(self):
        """검증 리포트 출력"""
        report = self.validation_report
        
        logger.info("\n" + "="*80)
        logger.info("🔍 KIS 데이터 검증 결과 리포트")
        logger.info("="*80)
        
        # 전체 결과
        overall = report['overall_result']
        status_emoji = {
            'EXCELLENT': '🟢',
            'GOOD': '🔵', 
            'ACCEPTABLE': '🟡',
            'FAIL': '🔴',
            'CRITICAL_FAIL': '❌'
        }
        
        logger.info(f"📊 전체 상태: {status_emoji.get(overall['overall_status'], '❓')} {overall['overall_status']}")
        logger.info(f"📈 성공률: {overall['success_rate']}% ({overall['passed_checks']}/{overall['total_checks']})")
        logger.info(f"⏱️  검증 시간: {report['metadata']['duration']}")
        
        if overall['critical_failures'] > 0:
            logger.warning(f"⚠️  심각한 오류: {overall['critical_failures']}개")
        
        # 데이터베이스 통계
        logger.info(f"\n📊 데이터베이스 통계:")
        for table, count in report['database_validation']['table_stats'].items():
            logger.info(f"  📋 {table}: {count:,}개")
        
        # 시장별 분포
        logger.info(f"\n📈 시장별 종목 분포:")
        market_dist = report['business_validation']['market_distribution']
        for market, types in market_dist.items():
            total_market = sum(types.values())
            logger.info(f"  🏢 {market}: {total_market:,}개")
            for inst_type, count in types.items():
                logger.info(f"    └ {inst_type}: {count:,}개")
        
        # 파일-DB 비교 결과
        logger.info(f"\n🔄 파일-DB 비교 결과:")
        count_comp = report['count_comparison']
        for item, comp in count_comp.items():
            status = "✅" if comp['is_match'] else "❌"
            if item == 'instruments_total':
                logger.info(f"  {status} 전체 종목: {comp['match_percentage']}% 일치")
            else:
                logger.info(f"  {status} {item}: {comp['match_percentage']}% 일치")
        
        # 별칭 통계
        alias_stats = report['business_validation']['alias_stats']
        logger.info(f"\n🏷️  별칭 통계:")
        logger.info(f"  📊 별칭 보유율: {alias_stats['alias_percentage']}%")
        logger.info(f"  📊 평균 별칭 수: {alias_stats['avg_alias_count']}개")
        
        # 대표 종목 검증
        known_checks = report['sample_validation']['known_stock_checks']
        passed_known = sum(1 for check in known_checks if check['result'])
        logger.info(f"\n[TARGET] 대표 종목 검증: {passed_known}/{len(known_checks)}개 통과")
        
        # 실패한 검증 항목 (상위 10개)
        failed_checks = self._collect_failed_checks()
        if failed_checks:
            logger.warning(f"\n❌ 실패한 검증 ({len(failed_checks)}개):")
            for i, check in enumerate(failed_checks[:10], 1):
                logger.warning(f"  {i}. {check}")
            if len(failed_checks) > 10:
                logger.warning(f"  ... 및 {len(failed_checks) - 10}개 더")
        else:
            logger.info(f"\n✅ 모든 검증 통과!")
        
        logger.info("="*80)
    
    def _collect_failed_checks(self):
        """실패한 검증 항목 수집"""
        failed_checks = []
        
        # 각 카테고리별 실패 항목 수집
        categories = [
            ('database_validation.data_type_checks', '데이터 타입'),
            ('database_validation.integrity_checks', '참조 무결성'),
            ('business_validation.market_validation', '시장 분포'),
            ('business_validation.code_format_checks', '코드 형식'),
            ('business_validation.completeness_checks', '데이터 완성도'),
            ('sample_validation.known_stock_checks', '대표 종목'),
            ('sample_validation.search_checks', '검색 기능'),
        ]
        
        for category_path, category_name in categories:
            checks = self._get_nested_value(self.validation_report, category_path)
            if checks and isinstance(checks, list):
                for check in checks:
                    if not check.get('result', False):
                        desc = check.get('description', check.get('check', '알 수 없음'))
                        failed_checks.append(f"{category_name}: {desc}")
        
        # 파일-DB 비교 실패 항목
        if 'count_comparison' in self.validation_report:
            for item, comparison in self.validation_report['count_comparison'].items():
                if not comparison.get('is_match', False):
                    failed_checks.append(f"레코드 수 비교: {item} (차이: {comparison.get('difference', 0):+,})")
        
        return failed_checks
    
    def save_report(self, filename='kis_validation_report.json'):
        """검증 리포트를 파일로 저장"""
        import json
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.validation_report, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"📄 검증 리포트가 {filename}에 저장되었습니다.")
        except Exception as e:
            logger.error(f"리포트 저장 실패: {e}")
    
    def get_summary(self):
        """검증 결과 요약 반환"""
        if not self.validation_report:
            return None
            
        overall = self.validation_report['overall_result']
        db_stats = self.validation_report['database_validation']['table_stats']
        
        return {
            'status': overall['overall_status'],
            'success_rate': overall['success_rate'],
            'total_instruments': db_stats.get('instruments', 0),
            'validation_time': self.validation_report['metadata']['duration'],
            'critical_failures': overall['critical_failures']
        }
