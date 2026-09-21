"""저장 계층과 화면이 함께 사용하는 증권사 지원 범위. 비밀·통신 코드는 없다."""

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Product:
    id: str
    label: str
    help: str
    overseas_live: bool = False
    overseas_mock: bool = False


@dataclass(frozen=True)
class BrokerDefinition:
    id: str
    name: str
    help: str
    products: tuple[Product, ...]
    environments: tuple[str, ...] = ("live", "mock")
    account_input: bool = False
    hts_id: bool = False
    activity: bool = False
    key_bound_account: bool = False
    select_environment: bool = True

    def public(self) -> dict:
        return asdict(self)


BROKERS = {
    "namuh": BrokerDefinition("namuh", "NH", "나무 OpenAPI 앱키로 연결 가능한 계좌를 확인합니다. 키는 서버에 암호화해 저장하며 주문은 실행하지 않습니다.", (
        Product("stocks", "주식·ETF", "국내 비상장·상장폐지 종목은 제외합니다. 원화는 D+2 예수금, 외화는 통화별 결제 후 예수금을 항상 가져옵니다.", True, True),
        Product("gold", "금현물", "금현물 전용 잔고를 조회합니다. 금은 g 단위, 원화 현금은 D+2 예수금 기준입니다."),
        Product("krfuture", "국내선물·옵션", "계약 수·매수·매도는 별도 표시하고 합계에는 증권사의 계좌 평가액을 반영합니다."),
        Product("gbfuture", "해외선물·옵션", "계약 수·매수·매도는 별도 표시하고 합계에는 증권사의 계좌 평가액을 반영합니다."),
    ), activity=True, select_environment=False),
    "kis": BrokerDefinition("kis", "한국투자증권", "본인 계좌와 전용 앱키를 입력하세요. HTS ID를 입력하면 서버에서 체결 통보를 받아 갱신합니다. 키는 암호화해 저장합니다.", (
        Product("stocks", "주식·ETF", "주식 계좌(상품코드 01)의 국내·해외 주식과 D+2 원화·결제 반영 외화 예수금을 가져옵니다. 모의계좌는 국내주식만 지원합니다. 금·선물·연금·CMA 잔고는 미지원입니다.", True),
    ), account_input=True, hts_id=True),
    "kiwoom": BrokerDefinition("kiwoom", "키움증권", "키움 REST API 전용 앱키를 입력하세요. API에서 확인한 계좌번호를 표시합니다. 서버의 접속 IP를 키움에 등록해야 합니다. 키는 암호화해 저장합니다.", (
        Product("stocks", "주식·ETF", "국내 상장주식·ETF와 미국주식, D+2 원화·결제 반영 USD 예수금을 가져옵니다. 모의계좌는 국내주식만 지원합니다. 다른 외화·신용·대출·선물·연금은 미지원입니다.", True),
        Product("gold", "금현물", "금현물 전용 앱키의 금 1kg 종목을 g 단위로 가져옵니다. 현금은 매매 정산을 반영한 추정예수금입니다. 미니금·선물은 미지원입니다."),
    ), key_bound_account=True),
    "ls": BrokerDefinition("ls", "LS증권", "LS Open API에 신청한 주식 계좌의 앱키를 입력하세요. API에서 계좌번호를 확인합니다. 키는 암호화해 저장합니다. 현재 실계좌 연결을 지원합니다.", (
        Product("stocks", "주식·ETF", "코스피·코스닥·ETF·CMA RP와 미국주식, D+2 원화·결제 반영 외화 예수금을 가져옵니다. 신용·대출·금·선물·연금은 미지원입니다.", True),
    ), environments=("live",), key_bound_account=True),
}
