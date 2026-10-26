# Distributed Banking Platform

**تکلیف دوم درس سیستم‌های توزیع‌شده** — Sharding, Replication & Distributed Transactions

## هدف

تبدیل سامانه بانکداری تکلیف اول به یک بانک توزیع‌شده که حساب‌ها را در دو شارد مستقل نگه می‌دارد، درخواست‌ها را بین Replicaهای سالم پخش می‌کند و انتقال بین شاردها را با **Two-Phase Commit (2PC) واقعی و قابل بازیابی** انجام می‌دهد. تمام سرویس‌ها باید با **Docker Compose روی یک میزبان** اجرا شوند.

> **وضعیت فعلی: فاز ۲ — کد شاردینگ پایدار آماده، تأیید Docker روی VPS باقی است.** جدول حساب‌ها، شناسنامه هر شارد، مقداردهی اولیه قابل‌تکرار و خواندن مستقیم از PostgreSQL در Replicaها پیاده‌سازی شده است. مسیریابی عملیاتی Gateway، واریز/برداشت، انتقال وجه، 2PC و بازیابی **هنوز پیاده‌سازی نشده‌اند**؛ برای آن‌ها پاسخ `501` طبیعی است.

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

## ساختار پروژه تا فاز ۲

| مسیر | نقش فعلی |
|---|---|
| `bank_platform/gateway/` | سرویس Gateway با Health Check؛ عملیات بانکی هنوز غیرفعال |
| `bank_platform/shard/` | سرویس Replicaها، مسیر تست خواندن حساب و بررسی آمادگی دیتابیس |
| `bank_platform/coordinator/` | اسکلت Coordinator؛ پیاده‌سازی 2PC در فازهای بعد |
| `bank_platform/common/validation.py` | اعتبارسنجی محلی برگرفته از تکلیف اول |
| `bank_platform/common/shard_map.py` | شناسایی مالک حساب با نقشه قطعی (فعلاً بدون HTTP routing) |
| `config/shard_map.json` | محدوده حساب‌ها و آدرس Replicaها |
| `compose.yaml` | ۸ سرویس اصلی + ۲ Job یک‌بارمصرف راه‌اندازی دیتابیس |
| `client/main.py` | کلاینت برگرفته از تکلیف اول با فرمان Health Check |
| `database/shard_a.sql` و `shard_b.sql` | ساخت جدول‌ها و داده اولیه، با حفظ موجودی موجود هنگام اجرای دوباره |
| `bank_platform/shard/storage.py` | خواندن واقعی PostgreSQL با بررسی شناسنامه دیتابیس و مالکیت حساب |
| `scripts/verify_phase2.py` | تأیید عملی دو دیتابیس و چهار Replica روی VPS |
| `tests/` | تست‌های محلی واحد و ساختاری؛ جایگزین اجرای PostgreSQL واقعی نیستند |

**توجه:** تنظیمات `max_prepared_transactions=100` از همین حالا برای دیتابیس‌ها قرار داده شده، اما این به معنی پیاده‌سازی 2PC نیست.

## تست‌های محلی

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

## اجرای فاز ۲ روی VPS (بدون دست زدن به Assignment 1)

از ریشه این ریپو روی VPS و پس از اطمینان از نصب Docker Compose:

```bash
bash scripts/preflight_vps.sh
cp .env.example .env  # فقط بار اول؛ اگر .env موجود است آن را بازنویسی نکنید
# رمزهای SHARD_A_DB_PASSWORD و SHARD_B_DB_PASSWORD را در .env تغییر بدهید.
docker compose config --quiet
docker compose up -d --build
docker compose ps -a
python3 scripts/verify_phase2.py
```

برای اثبات پایداری داده بعد از Restart دیتابیس‌ها:

```bash
python3 scripts/verify_phase2.py --restart-check
```

اسکریپت این موارد را بررسی می‌کند: وجود ۱۰۰۰ حساب در هر دیتابیس، عدم هم‌پوشانی، رد نوشتن روی شارد نامربوط توسط PostgreSQL، مقدار اولیه A0500=1000 و A1500=500، و پاسخ یکسان Replicaهای A1/A2 و B1/B2. اجرای مجدد Jobهای مقداردهی اولیه **موجودی حساب‌های قبلی را بازنویسی نمی‌کند**.

توجه: مسیر `GET /internal/accounts/{account}` فقط یک مسیر داخلی و صرفاً خواندنی برای اثبات ذخیره‌سازی در فاز ۲ است؛ درگاه عمومی (Gateway) همچنان عملیات بانکی را با `501` رد می‌کند. هیچ پورتی برای دیتابیس یا Replica روی میزبان منتشر نشده است.

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
