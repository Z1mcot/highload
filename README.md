# DDIA HW2 — стратегии устойчивости на Rust

Учебная библиотека `resilience`: default, retries, exponential backoff, round-robin,
hedging и round-robin с circuit breaker. Сервис модельный; сеть не используется.
Требования находятся в [исходном задании](ITMO-DDIA-2026-MSE_MHS-HW-2.md), решения —
в [ARCHITECTURE.md](ARCHITECTURE.md).

## Запуск

Нужен стабильный Rust с Cargo, rustfmt и Clippy (edition 2021; проверено на Rust
1.99.1). При первой сборке Cargo скачает Tokio, async-trait и их зависимости.
`Cargo.lock` фиксирует версии; после скачивания тесты работают без сети.

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all-targets
./test.sh
```

Исполняемый `test.sh` запускает `cargo test --locked --all-targets` и
`cargo test --locked --doc`. Примеры API из этого README также проверяются как
документационные тесты. Временные сценарии используют виртуальные часы Tokio.

## API

`SubrequestSender::send(&self)` асинхронно возвращает `ServerOutcome::Success`
с телом `Vec<u8>`, `ClientError` (4xx) или `ServerError` (5xx). Зависание sender
превращается в timeout на стороне клиента. Метод не принимает таймауты или
настройки стратегии. Реализация должна уступать управление runtime и освобождать
ресурсы при удалении future; отмена ожидания не гарантирует отмену работы удалённого
сервиса. Fake sender для тестов находится в `tests/support/mod.rs`.

`Client` хранит проверенную конфигурацию, а sender передаётся на один логический
запрос. Например, если `sender` реализует `SubrequestSender`:

```rust
use std::time::Duration;
use resilience::{
    Client, ClientError, ExponentialBackoff, LatencyBudget, RetryBudget,
    SingleStrategy, SubrequestSender,
};

# async fn example(sender: &dyn SubrequestSender) -> Result<(), ClientError> {
let latency = LatencyBudget {
    overall: Duration::from_millis(200),
    per_attempt: Duration::from_millis(30),
};
let budget = RetryBudget {
    max_attempts: 5,
    max_client_errors: 2,
    max_server_errors: 3,
    max_timeouts: 2,
};
let strategy = SingleStrategy::Backoff {
    budget,
    backoff: ExponentialBackoff {
        initial_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(40),
    },
};
let client = Client::new(latency, strategy)?;
let response = client.execute(sender).await?;
# let _ = response;
# Ok(())
# }
```

Для одной попытки используется `SingleStrategy::Default`, для повторов без пауз —
`SingleStrategy::Retry(budget)`. Полезная нагрузка 2xx сохраняется без изменений,
включая пустое тело.

`MultiClient::execute(&mut self, senders)` принимает срез
`Arc<dyn SubrequestSender>`. Пустой список возвращает `ClientError::NoSenders`.
Одновременно один экземпляр обслуживает один логический запрос; внутри hedging
подзапросы конкурентны. Пример для списка `senders`:

```rust
use std::{sync::Arc, time::Duration};
use resilience::{
    CircuitBreakerConfig, ClientError, LatencyBudget, MultiClient, MultiStrategy,
    SingleStrategy, SubrequestSender,
};

# async fn example(senders: &[Arc<dyn SubrequestSender>]) -> Result<(), ClientError> {
let mut client = MultiClient::new(
    LatencyBudget {
        overall: Duration::from_millis(200),
        per_attempt: Duration::from_millis(30),
    },
    MultiStrategy::CircuitBreaking {
        strategy: SingleStrategy::Default,
        breaker: CircuitBreakerConfig {
            window_size: 4,
            failure_threshold: 0.5,
            cooldown: Duration::from_secs(1),
        },
    },
)?;
let response = client.execute(senders).await?;
# let _ = response;
# Ok(())
# }
```

Чтобы сохранять курсор и историю breaker, сохраните `client` для последующих
вызовов; sender-объекты создавайте для каждого логического запроса. Сервер определяется
позицией в списке, поэтому для breaker число и порядок серверов должны оставаться
постоянными. Изменение числа возвращает `InvalidConfiguration` без сброса истории;
соответствие самих серверов позициям обеспечивает вызывающий код.

Вместо breaker можно выбрать `MultiStrategy::RoundRobin(policy)` с любой
`SingleStrategy`, в том числе backoff, или
`MultiStrategy::Hedging { hedging_delay: Duration::from_millis(10) }`.

## Правила бюджетов и времени

- Каждый `execute` создаёт один монотонный общий дедлайн. Попытка ограничена
  `min(per_attempt, remaining_overall_time)`; паузы входят в общий бюджет.
- `max_attempts` — число **всех** стартовавших подзапросов, включая первый.
  `max_attempts=1` соответствует формулировке задания `max_retries=1` для default.
- Ошибка категории увеличивает её счётчик. Достижение лимита запрещает следующую
  попытку: при лимите 2 после первой ошибки можно повторить, после второй нельзя.
  Лимиты 0 и 1 оба останавливают запрос после первой ошибки данной категории;
  нулевой лимит одной категории не запрещает попытки после ошибок другой категории.
- 4xx разрешено повторять в пределах бюджета: это выбранная трактовка fast errors
  budget. Backoff после 4xx отсутствует; 4xx также не сбрасывает рост задержек.
- После 5xx/timeout паузы равны `initial`, `2*initial`, `4*initial`, … до `max_delay`.
  Переполнение длительности насыщается; после исчерпания бюджета последней паузы нет.
- На точной границе времени дедлайн имеет приоритет над ответом. Если истёк общий
  дедлайн, возвращается `DeadlineExceeded`, а не timeout отдельной попытки.
- Default возвращает `AttemptFailed` с категорией ошибки; retry/backoff при
  исчерпании лимитов возвращают `BudgetExhausted` с последней ошибкой.
  В ошибках `RequestStats` отдельно хранит старты, 4xx, 5xx и per-attempt timeout.
  Старты могут превышать сумму исходов, если незавершённая попытка остановлена
  общим дедлайном. Счётчики относятся только к текущему `execute`.
- Нулевые длительности, нулевой общий лимит попыток, некорректные параметры backoff
  и breaker отклоняются без вызовов sender. `per_attempt > overall` допустимо.

## Особенности мультиклиента

**Round-robin.** Курсор меняется на каждой попытке и сохраняется между запросами.
Повтор после ошибки отправляется следующему серверу. Все попытки выполняет тот
же цикл retry, который использует одиночный клиент.

**Hedging.** Первый sender начинает сразу. Если к `hedging_delay` не пришёл
**никакой ответ**, остальные начинают конкурентно, каждый не более одного раза.
Ранний 4xx/5xx запрещает fanout; при отсутствии успеха запрос ждёт общий дедлайн,
даже когда все подзапросы уже завершились ошибками. Готовый ответ на границе
`hedging_delay` проверяется прежде запуска остальных; общий дедлайн имеет высший
приоритет. Нулевая задержка разрешена. Учитывается только общий latency budget;
`per_attempt` здесь не применяется. Первый 2xx завершает запрос и удаляет futures
проигравших. Фоновых задач нет, отмена вызывающего future тоже освобождает все
ожидания. При одновременно готовых успехах выбирается первый в порядке списка.

**Circuit breaker.** Для каждого сервера хранится окно последних N завершённых
попыток. После заполнения окна доля 4xx/5xx/timeout **строго больше** порога открывает
breaker на T. Попытка, прерванная общим дедлайном, также является ошибкой здоровья,
но не увеличивает счётчик per-attempt timeout. Отмена вызывающим кодом не добавляет
исход в окно. Открытые серверы пропускаются; если доступны ноль серверов, немедленно
возвращается `NoAvailableSenders` без ожидания cooldown. После T допускается одна
half-open проба: успех закрывает breaker и очищает окно, ошибка снова открывает на T.
Отменённую пробу можно повторить при следующем вызове. Состояние живёт в экземпляре
`MultiClient`; `&mut self` исключает конкурирующие пробы без mutex через `await`.

## Соответствие требованиям

| Требование | Реализация | Проверка поведения |
| --- | --- | --- |
| Sender, 2xx/4xx/5xx/timeout, отмена | `src/transport.rs`, `src/model.rs`, `src/strategies/default.rs` | `tests/default.rs`: payload, классификация, отмена и границы времени |
| Default: ровно один подзапрос | `SingleStrategy::Default`, общий цикл в `src/strategies/retry.rs` | `returns_success_payload_with_exactly_one_attempt`, `stops_after_one_client_or_server_error` |
| Retry: все бюджеты, единый дедлайн | `src/budget.rs`, `src/strategies/retry.rs` | `tests/retry.rs`: все категории, нулевые лимиты, mixed failures, общий дедлайн |
| Backoff без копирования retry | `src/strategies/backoff.rs`, `src/strategies/retry.rs` | `tests/backoff.rs`: удвоение и cap, 4xx без пауз, дедлайн во сне |
| Round-robin на каждой попытке | `src/strategies/routing.rs`, `src/client.rs` | `tests/round_robin.rs`: порядок, wraparound, ошибки, композиция backoff |
| Hedging и очистка проигравших | `src/strategies/hedging.rs` | `tests/hedging.rs`: время fanout, ранние ошибки, победители, deadline, отмена |
| Breaker, cooldown, half-open | `src/strategies/circuit_breaker.rs` | `tests/circuit_breaker.rs`: окно, порог, исключение, возврат, all-open, отменённая проба |
| Валидация без panic | Конструкторы, `src/budget.rs`, конфигурации стратегий | Тесты `rejects_*` во всех шести файлах тестов |
| Запуск из test.sh | `test.sh`, `Cargo.lock` | `./test.sh` запускает integration tests и doctests |

Библиотека предполагает работающий Tokio runtime с включёнными таймерами. Блокирующий
transport нельзя принудительно прервать асинхронным таймером. Настоящий HTTP transport,
jitter и совместные конкурентные вызовы одного `MultiClient` в учебную модель не входят.

Для сдачи по заданию владелец создаёт PR в личном GitHub-репозитории и отправляет
ссылку в Google Classroom.
