"""실시간 이커머스 이벤트 스트림 생성기.

배치 생성기(ecommerce-data-generator)가 "과거 1년치를 한 번에" 만든다면,
이 패키지는 wall-clock 시간이 실제로 흐르면서 세션이 계속 태어나고
각 세션이 퍼널을 따라 이벤트를 흘려보내는 실시간(스트리밍) 생성기다.

핵심 모듈:
- base       : 공유 유틸 (시드 RNG, 시간대 가중치)
- clock      : 실시간/시뮬레이션 시계 추상화
- dimensions : users/products 차원 풀 (메모리 생성 또는 parquet 로드)
- traffic    : 시간대 가중 세션 도착 과정(Poisson)
- sessions   : 세션/퍼널 상태머신 (이벤트 계획 수립)
- records    : event/order/payment 레코드 빌더
- sinks      : 플러그인형 출력(stdout/file/kafka)
- engine     : 위를 결합한 이산사건(discrete-event) 루프
"""

__version__ = "0.1.0"
