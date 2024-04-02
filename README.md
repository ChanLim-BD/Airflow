![](https://velog.velcdn.com/images/chan9708/post/34ed24fc-872b-4aa7-8bcd-4799c46b24d7/image.png)


# Apache Airflow?

![](https://velog.velcdn.com/images/chan9708/post/7724119e-4002-40c1-95c4-2cdf47c058e4/image.png)

> **_Python을 이용해
`Workflow`를 만들고 관리할 수 있는
`Open-Source` 기반 Workflow 관리 도구_**

## a.k.a

> **_<span style="color: red">Workflow as code</span>_**

---

## 특징

> **_Dynamic_** 
- Airflow 파이프라인이 Python 코드로 구성되어 동적 파이프라인 생성이 가능합니다.

> **_Extensive_** 
- Airflow™ 프레임워크에는 다양한 기술과 연결하는 연산자가 포함되어 있습니다. 모든 Airflow 구성요소는 확장 가능하여 환경에 맞게 쉽게 조정할 수 있습니다.

> **_Flexible_** 
- Jinja 템플릿 엔진을 활용하여 워크플로 매개변수화가 내장되어 있습니다 .



![](https://velog.velcdn.com/images/chan9708/post/c671f195-4ff4-4bcf-9c75-b9a99f761d72/image.png)

> **다시 말해서,**
>>
* **_Python으로 제작된 도구_**
>>
>>
* **_하나의 Workflow는 DAG (Directed Acycle Graph)_**
  * **DAG 안에는 1개 이상의 Task가 존재**
>>
>>
* **_Task간 선/후행 연결이 가능하되, 순환되지 않고 방향성 소유_**
>>
>>
* **_Cron 기반 스케쥴링_**
>>
>>
* **_Monitoring 및 실패 작업에 대한 재실행 기능이 간편_**

---

## 장점

* **_Python 기반이므로, Python을 알고 있다면 적응하기 쉬움_**

* **_대규모 Workflow에 적합한 
(K8S 아키텍처)등 수평적 확장이 가능한 아키텍처 지원_**

* **_Python의 방대한 라이브러리를 활용한 기능 지원
<span style="color: red">즉, Python으로 할 수 있는 모든 작업을 할 수 있다.</span>_**

* **_Customizing_**

---

## 단점

* **_<span style="color:red">실시간 Workflow에 적합하지 않는다.</span>_**

* **_DAG 개수가 많아지면 모니터링이 어려움**

* **_Workflow를 GUI에서 만들지 않아서 Coding이 요구됨_**

---

# Code 예시

```python
from airflow import DAG
import datetime
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_t1 = BashOperator(
        task_id="bash_t1",
        bash_command="echo whoami",
    )

    bash_t2 = BashOperator(
        task_id="bash_t2",
        bash_command="echo $HOSTNAME",
    )

    # Task 수행 순서 설정
    bash_t1 >> bash_t2
```
