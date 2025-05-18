import json

# 경로 설정
input_path = '../data/서울시_테니스장_정보.json'
output_path = '../processed_data/테니스장_상세.json'

# 원본 JSON 불러오기
with open(input_path, encoding='utf-8') as f:
    original = json.load(f)

# 유지할 키 목록 (v_min/v_max 은 별도 처리)
keys_to_keep = [ 'svcid', 'svcnm', 'usetgtinfo']
seen = set()

# 데이터 필터링 및 시간 합치기
processed_data = []
for item in original.get('DATA', []):
    # v_min과 v_max를 합쳐서 하나의 문자열 생성
    time_str = f"{item.get('v_min', '')}~{item.get('v_max', '')}"
    # 나머지 필터링
    filtered = {k: item.get(k, '') for k in keys_to_keep}
    # 합친 시간 필드 추가
    filtered['time'] = time_str
    processed_data.append(filtered)

# 새로운 구조 생성
cleaned = {
    'DESCRIPTION': original.get('DESCRIPTION', {}),
    'DATA': processed_data
}

# 결과 저장
with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(cleaned, f, ensure_ascii=False, indent=2)

print(f"정제된 JSON이 '{output_path}'에 저장되었습니다.")
