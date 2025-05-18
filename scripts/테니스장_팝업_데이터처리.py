import json

# 경로 설정
input_path = '../data/서울시_테니스장_정보.json'
output_path = '../processed_data/테니스장_팝업.json'

# 원본 JSON 불러오기
with open(input_path, encoding='utf-8') as f:
    original = json.load(f)

# 유지할 키 목록
keys_to_keep = ['placenm', 'telno', 'lat', 'lng', 'imgurl']
seen = set()  # 중복 체크를 위한 set

# 데이터 필터링 및 중복 제거
processed_data = []
for item in original.get('DATA', []):
    # lat 값 추출 (문자열로 처리하여 정확한 비교)
    lat = str(item.get('lat', ''))
    
    # 이미 처리된 lat 값이면 건너뛰기
    if lat in seen:
        continue
        
    # 새로운 lat 값 추가
    seen.add(lat)
    
    # 필터링
    filtered = {k: item.get(k, '') for k in keys_to_keep}
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
print(f"총 {len(original.get('DATA', []))}개의 데이터 중 {len(processed_data)}개의 고유 데이터가 저장되었습니다.")