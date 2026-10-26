# Distributed Banking Platform

**تکلیف دوم درس سیستم‌های توزیع‌شده** — Sharding, Replication & Distributed Transactions

## هدف

تبدیل سامانه بانکداری تکلیف اول به یک بانک توزیع‌شده که حساب‌ها را در دو شارد مستقل نگه می‌دارد، درخواست‌ها را بین Replicaهای سالم پخش می‌کند و انتقال بین شاردها را با **Two-Phase Commit (2PC) واقعی و قابل بازیابی** انجام می‌دهد. تمام سرویس‌ها باید با **Docker Compose روی یک میزبان** اجرا شوند.

> **وضعیت فعلی: فاز ۴ — عملیات محلی اتمیک و انتقال داخل شارد پیاده‌سازی شده‌اند و تست محلی پاس شده است؛ پذیرش عملی فاز ۴ روی VPS باقی است.** فازهای ۲ و ۳ با آزمایش روی VPS پذیرفته شده‌اند. انتقال بین دو شارد، 2PC و بازیابی Coordinator هنوز آماده نیستند؛ انتقال بین‌شاردی عمداً پاسخ `501` می‌دهد.

## معماری هدف

```text
Banking Client
      |
      v
Gateway (shard lookup + healthy replica selection)
      |                              |
      |                              v
      |                        Coordinator (2PC)
      |                              |
      +---- Shard A (A0001..A1000) -+---- Shard B (A1001..A2000)
             |        |                      |        |
           Bank A1  Bank A2                Bank B1  Bank B2
              \      /                        \      /
            PostgreSQL A                    PostgreSQL B
```

هر دو Replica در یک شارد به دیتابیس **مشترک همان شارد** وصل می‌شوند؛ دیتابیس‌های A و B مستقل‌اند. Gateway تنها ورودی عمومی سامانه است. Coordinator در پایان فازهای ۵ و ۶ تصمیمات تراکنش را به‌شکل پایدار ثبت و بازیابی خواهد کرد.

## ساختار پروژه تا فاز ۴

| مسیر | نقش فعلی |
|---|---|
| `bank_platform/gateway/` | مسیریابی همه عملیات محلی، Round Robin در هر شارد، بررسی سلامت Replica و Failover فقط برای خواندن |
| `bank_platform/shard/` | سرویس Replicaها، عملیات واریز/برداشت و انتقال اتمیک داخلی، بررسی آمادگی دیتابیس |
| `bank_platform/coordinator/` | اسکلت Coordinator؛ پیاده‌سازی 2PC در فازهای بعد |
| `bank_platform/common/validation.py` | اعتبارسنجی Balance/Deposit/Withdraw و Transfer داخلی |
| `bank_platform/common/shard_map.py` | شناسایی قطعی شارد مالک حساب برای مسیریابی HTTP |
| `config/shard_map.json` | محدوده حساب‌ها و آدرس Replicaها |
| `compose.yaml` | ۸ سرویس اصلی + ۲ Job یک‌بارمصرف راه‌اندازی دیتابیس |
| `client/main.py` | کلاینت برگرفته از تکلیف اول با فرمان Health Check |
| `database/shard_a.sql` و `shard_b.sql` | ساخت جدول‌ها و داده اولیه، با حفظ موجودی موجود هنگام اجرای دوباره |
| `bank_platform/shard/storage.py` | خواندن و نوشتن PostgreSQL با قفل‌گذاری سطری، تراکنش اتمیک و بررسی مالکیت حساب |
| `scripts/verify_phase2.py` | تأیید عملی دو دیتابیس و چهار Replica روی VPS (پاس شده) |
| `scripts/verify_phase3.py` | تست واقعی توزیع بار، مسیریابی و قطع یک Replica روی VPS (پاس شده) |
| `scripts/verify_phase4.py` | تست واقعی واریز/برداشت موازی و انتقال داخلی، بدون تغییر خالص موجودی تست‌ها |
| `tests/` | تست‌های محلی واحد و ساختاری؛ جایگزین اجرای PostgreSQL واقعی نیستند |

**توجه:** تنظیمات `max_prepared_transactions=100` از همین حالا برای دیتابیس‌ها قرار داده شده، اما این به معنی پیاده‌سازی 2PC نیست.

## تست‌های محلی

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

## اجرای فاز ۴ روی VPS (بدون دست زدن به Assignment 1)

از ریشه این ریپو روی VPS و پس از اطمینان از نصب Docker Compose:

```bash
bash scripts/preflight_vps.sh
cp .env.example .env  # فقط بار اول؛ اگر .env موجود است آن را بازنویسی نکنید
# رمزهای SHARD_A_DB_PASSWORD و SHARD_B_DB_PASSWORD را در .env تغییر بدهید.
docker compose config --quiet
docker compose up -d --build
docker compose ps -a
python3 scripts/verify_phase2.py
python3 scripts/verify_phase3.py
# با توقف عمدی bank_a1 و راه‌اندازی مجدد آن:
python3 scripts/verify_phase3.py --failover
python3 scripts/verify_phase4.py
```

برای اثبات پایداری داده بعد از Restart دیتابیس‌ها:

```bash
python3 scripts/verify_phase2.py --restart-check
```

اسکریپت این موارد را بررسی می‌کند: وجود ۱۰۰۰ حساب در هر دیتابیس، عدم هم‌پوشانی، رد نوشتن روی شارد نامربوط توسط PostgreSQL، مقدار اولیه A0500=1000 و A1500=500، و پاسخ یکسان Replicaهای A1/A2 و B1/B2. اجرای مجدد Jobهای مقداردهی اولیه **موجودی حساب‌های قبلی را بازنویسی نمی‌کند**.

مسیر `GET /internal/accounts/{account}` داخلی است. درگاه عمومی `GET /api/v1/accounts/{account}/balance` و `POST /api/v1/operations` را به Replica سالم شارد درست هدایت می‌کند. عملیات `deposit`، `withdraw` و `transfer` داخلی پشتیبانی می‌شوند؛ انتقال بین‌شاردی تا پیاده‌سازی 2PC غیرفعال است. هیچ پورت دیتابیس یا Replica روی میزبان منتشر نشده است.

سرویس‌های `shard_a_init` و `shard_b_init` بعد از ساخت جدول و Seed با وضعیت Exit(0) متوقف می‌شوند؛ این دو **جزء سرویس‌های دائماً فعال نیستند**. برای اجرای مجدد SQL روی دیتابیس‌های قبلی:

```bash
docker compose run --rm shard_a_init
docker compose run --rm shard_b_init
```

**ایمنی:** نام پروژه Docker Compose برابر `distributed-banking` است؛ داده Assignment 1 دست‌نخورده می‌ماند. به‌هیچ‌وجه `docker compose down -v` یا `docker volume prune` اجرا نکنید. برای توقف بدون حذف داده‌ها `docker compose down` کافی است.

## ترتیب توسعه (بدون Issue؛ Commit محور)

| فاز | خروجی الزامی | Commit پیشنهادی |
|---|---|---|
| ۱ | ساختار مستقل، Compose، شناسایی اجزا، تست پایه | `chore: initialize distributed banking scaffold` |
| ۲ | دو دیتابیس، ایجاد حساب‌ها، مالکیت یکتا و ذخیره‌سازی واقعی | `feat: implement persistent account sharding` |
| ۳ | Gateway، مسیریابی، توزیع بار و خروج Replica ناسالم | `feat: route requests to healthy shard replicas` |
| ۴ | Balance/Deposit/Withdraw، هم‌زمانی و انتقال داخل شارد | `feat: implement atomic local banking operations` |
| ۵ | Coordinator و پروتکل واقعی PREPARE/COMMIT/ABORT | `feat: implement cross-shard two-phase commit` |
| ۶ | وضعیت پایدار تراکنش، استعلام و سناریوهای بازیابی A/B/C | `feat: recover unresolved distributed transactions` |
| ۷ | اجرای ۷ آزمایش اجباری و جمع‌آوری شواهد | `test: verify mandatory distributed banking scenarios` |
| ۸ | استقرار روی VPS، Docker Hub، گزارش و ویدئو | `chore: prepare assignment submission` |
| ۹ (اختیاری) | Autonomous Self-Healing؛ بدون restart policy خودکار | `feat: implement autonomous replica recovery` |
| ۱۰ (اختیاری) | افزودن Shard C و مهاجرت زنده با Epoch و Fencing | `feat: implement live resharding` |

هر فاز فقط با پاس شدن معیارهای پذیرش همان فاز تکمیل‌شده در نظر گرفته می‌شود. برای جزئیات گیت‌ها فایل `docs/IMPLEMENTATION_PLAN.md` را ببینید.

## هفت آزمایش اصلی استاد

۱. توزیع حساب‌ها و مسیریابی درست؛ ۲. پخش درخواست بین Replicaها؛ ۳. صحت موجودی زیر بار هم‌زمان؛ ۴. ادامه سرویس با قطع یک Replica؛ ۵. انتقال داخلی و بین‌شاردی؛ ۶. بازیابی Coordinator بعد از ثبت تصمیم COMMIT و پیش از همه تأییدها؛ ۷. انتقال‌های هم‌زمان بین شاردها با حساب‌های مشترک و حفظ مجموع پول.

## امکانات امتیازی

- **+۱۰٪:** ساخت کنترل‌کننده Self-Healing مستقل با بازیابی از Crash؛ Kubernetes، Docker Swarm، ابزارهای آماده خودترمیمی و Docker automatic restart ممنوع‌اند.
- **+۱۲٪:** افزودن شارد C و مهاجرت آنلاین `A1501..A2000` از B، شامل Change Capture، Epoch/Fencing، سازگاری با 2PC و بازیابی خودکار مهاجرت.

## خروجی نهایی تکلیف

سورس در GitHub، تمام ایمیج‌های ضروری در Docker Hub، گزارش فنی حداکثر ۱۰ صفحه A4 و ویدئو حداکثر ۱۵ دقیقه. موعد سند: `۱۴۰۵/۰۸/۱۵`؛ تحویل: LMS، GitHub و Docker Hub.

**مرجع اصلی:** `Assignment2_Distributed_Banking_Platform.pdf`. کد تکلیف اول صرفاً به‌عنوان مبنای مهندسی استفاده شده و پروژه فعلی هیچ فایل یا Volume آن را تغییر نمی‌دهد.

## یادداشت فاز ۳

- `GET /health` صرفاً سلامت فرایند Gateway است و `banking_ready=false` تا پیاده‌سازی تراکنش‌ها باقی می‌ماند.
- `GET /ready` زمانی ۲۰۰ می‌دهد که برای **هر شارد** حداقل یک Replica آماده وجود داشته باشد.
- Round Robin برای A و B جداست؛ انتخاب Replica ناسالم متوقف می‌شود و بعد از بازیابی دوباره وارد چرخه می‌شود.
- تنها استعلام‌های **خواندنی** قابلیت تلاش مجدد روی Replica دیگر دارند؛ Retry نوشتن پول نیازمند طراحی فاز ۴ است.
- جزئیات تست قطع کانتینر: `docs/PHASE3_ACCEPTANCE.md`.

## عملیات محلی فاز ۴

```json
{"action":"deposit","account":"A0701","amount":10}
{"action":"withdraw","account":"A0701","amount":5}
{"action":"transfer","from_account":"A0701","to_account":"A0702","amount":5}
```

هر درخواست نوشتنی تنها به یک Replica در شارد مالک فرستاده می‌شود. قفل `SELECT ... FOR UPDATE` روی PostgreSQL مشترک از Lost Update جلوگیری می‌کند. هر دو طرف انتقال داخلی در یک تراکنش دیتابیس Commit می‌شوند یا هر دو Rollback می‌شوند.

**محدودیت فعلی:** در صورت Timeout یا قطع ارتباط هنگام عملیات پولی، Gateway پاسخ `504 UNKNOWN_OUTCOME` می‌دهد و درخواست را به Replica دیگر تکرار نمی‌کند. تا زمان پیاده‌سازی شناسه درخواست و استعلام وضعیت پایدار در مراحل بعد، کاربر نباید درخواست مالی نامشخص را کورکورانه تکرار کند. `banking_ready=false` به معنای ناتمام بودن کل عملیات بانکداری توزیع‌شده از جمله 2PC است، نه ناموجود بودن عملیات محلی.

آزمایش‌ها و معیار پذیرش: `docs/PHASE4_ACCEPTANCE.md`.


## Phase 5 -- cross-shard 2PC (pending VPS acceptance)

Cross-shard transfers now use a durable Coordinator decision journal (SQLite,
existing `coordinator_state` volume) and PostgreSQL native `PREPARE TRANSACTION`
with `COMMIT PREPARED` / `ROLLBACK PREPARED`. Participant receipts are committed
atomically with account updates; duplicate client transaction IDs cannot debit
a second time. The Gateway publishes an operation status lookup:

```bash
# Run only against a deployed Phase 5 Gateway on VPS:
curl -X POST http://127.0.0.1:8001/api/v1/operations \
  -H 'Content-Type: application/json' \
  -d '{"action":"transfer","from_account":"A0801","to_account":"A1801","amount":5,"transaction_id":"11111111-1111-4111-8111-111111111111"}'
curl http://127.0.0.1:8001/api/v1/transactions/11111111-1111-4111-8111-111111111111
```

For verified, balance-neutral demonstrations use `python3 scripts/verify_phase5.py`
(instead of the manual sample above). See `docs/PHASE5_ACCEPTANCE.md` for
operational caution about `PENDING` outcomes and outstanding prepared GIDs.
**Do not use this version as a fault-tolerant production service until Phase 6
crash recovery is implemented and tested.**
