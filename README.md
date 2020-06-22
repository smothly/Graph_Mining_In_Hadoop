# Graph_Mining_In_Hadoop
MapReduce 사용하여 그래프 내에서 각 노드의 군집계수 찾기

- GCP dataproc 사용
- 4개의 과정으로 나누어 진행
  - 1. 중복 edge & self loop 제거 
  - 2. 각 노드의 degree 구하기
  - 3. 각 노드의 삼각형 개수 구하기
  - 4. 각 노드의 긴집 계수 구하기

java와 scala로 각각 구현
