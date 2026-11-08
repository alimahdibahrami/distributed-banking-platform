# برنامه مشترک بونوس‌های ۹ و ۱۰ — سامانه بانکداری توزیع‌شده

مبنای این سند: صورت تکلیف دوم، بخش‌های ۷ و ۸؛ سورس Commit پایدار `a9759ca` و نتایج فازهای ۱ تا ۸B.  این بسته **هیچ داده بانکی را جابه‌جا نمی‌کند**.

## تصمیم معماری و مرز دو بونوس

بونوس ۹ کنترل سلامت و جایگزینی کانتینرهای Replica را مستقل از بونوس ۱۰ اجرا می‌کند؛ شاردها به PostgreSQL مشترک خود متصل‌اند و جایگزینی Replica نباید داده یا Coordinator را تغییر دهد. بونوس ۱۰ نیازمند تغییر مالکیت داده، پروتکل 2PC و قراردادهای نوشتن است و نمی‌تواند با یک تغییر ساده `shard_map.json` معتبر شود.

## گام‌های پیشنهادی، مستقل اما در یک برنامه

| گام | تحویل | معیار پذیرش | ریسک |
|---|---|---|---|
| B0 | Checkpoint و گزارش baseline | ۷/۷ تست اصلی؛ صفر Prepared؛ Image پایدار نگه داشته شود | پایین |
| B9.1 | Self-Healing Controller مستقل | تشخیص Unhealthy/Stopped/Missing و جایگزینی فقط Replica هدف | متوسط |
| B9.2 | بازیابی Controller | مرگ Controller بعد از ایجاد کانتینر باعث Duplicate نشود | متوسط |
| B9.3 | آزمایش تحت خواندن و نوشتن هم‌زمان | عدم توقف Replica سالم؛ شاهد ID جدید و برگشت به Round Robin؛ سلامت تراکنش‌ها | متوسط |
| B10.1 | پشتیبان و Schema نسخه ۲ | C با دیتابیس مستقل و دو Replica؛ بدون فعال‌سازی مسیر C | متوسط |
| B10.2 | CDC و Snapshot | ثبت تغییرهای **مرتبط با COMMIT** با ترتیب پایدار، Copy و Replay idempotent | زیاد |
| B10.3 | 2PC نسخه‌دار | ثبت مشارکت‌کنندگان و Epoch در هر تراکنش؛ Recovery با مالکیت تاریخی | خیلی زیاد |
| B10.4 | Epoch Fence + Cutover | بستن نوشتن B در همان تراکنش SQL و فعال‌سازی اتمیک C بعد از Catch-up | خیلی زیاد |
| B10.5 | بازیابی ۴ Crash Gate | Crash در Snapshot، Replay، Fencing و پس از Activation؛ عدم دو مالک نوشتنی | خیلی زیاد |
| B10.6 | آزمون نهایی | ۵۰۰ حساب A1501..A2000 در C، B فاقد مالکیت، انتقال‌ها و conservation | خیلی زیاد |

**سیاست ایمنی:** فاز ۹ قابل استقرار روی VPS پایدار است؛ فاز ۱۰ تا تکمیل تمام B10.1–B10.5 فقط مدل و پیش‌آزمون دارد. هیچ SQL پاک‌کننده یا Cutover در بسته حاضر نیست. برای بونوس ۱۰ ابتدا محیط آزمایش جدا یا کپی مستقل Volumeها استفاده شود.

## بونوس ۹: سازوکار اجرا

- کنترل‌کننده خارج از کانتینرها به‌صورت فرایند systemd اختصاصی اجرا می‌شود؛ نه Kubernetes، Swarm یا Docker Auto Restart. `restart: "no"` برای هر Replica باقی می‌ماند.
- هر ۵ ثانیه چهار سرویس مجاز `bank_a1`, `bank_a2`, `bank_b1`, `bank_b2` را بررسی می‌کند. پس از دو مشاهده خرابی، و با Cooldown و Startup Grace، تنها همان سرویس را با Docker Compose و `--no-deps --force-recreate` جایگزین می‌کند.
- نام سرویس/شبکه Compose ثابت می‌ماند و Gateway با Health Poll موجود، Replica جایگزین را به Round Robin برمی‌گرداند. نام کانتینر/برچسب‌های Compose برای جلوگیری از هدف‌گرفتن پروژه دیگر کنترل می‌شوند.
- Intent و نتیجه در فایل JSON با ذخیره اتمیک و `fsync` ثبت می‌شود. اگر Controller هنگام Replace متوقف شود، پس از Restart، وضعیت واقعی Docker را می‌خواند؛ وجود Replica Healthy موجب Replace دوم نمی‌شود. قفل انحصاری `flock` مانع دو Controller هم‌زمان است.
- جایگزینی کانتینر، دیتابیس یا Prepared transaction را پاک نمی‌کند و عملیات `down -v`، `prune` و Restart عمومی هرگز استفاده نمی‌شود.
- **محدودیت:** جایگزینی سرویس طبق Compose از همان Image انجام می‌شود. از دسترس رفتن خود Docker daemon یا دیتابیس با این کنترل‌کننده حل نمی‌شود. Self-Healing به معنی تضمین موفقیت همه Writes نامطمئن نیست؛ UUID و Status Query ضروری‌اند.
- **ملاحظات امنیتی:** کنترل‌کننده روی Host با دسترسی Docker و Root اجرا می‌شود. فایل و باینری باید مورد اعتماد باشند، مسیر سرویس‌ها ثابت بماند و دسترسی به مخزن کد محدود باشد.

## اجرای کنترل‌شده بونوس ۹ روی VPS

ابتدا ZIP بسته را فقط در `/root` منتقل کنید و سورس را با اسکریپت نصب همراه بسته کپی کنید. اسکریپت سورس اصلی را بازنویسی نمی‌کند. پس از موفقیت آزمون Read-Only:

```bash
cd /opt/distributed-banking-platform
python3 scripts/self_healing_controller.py --once --dry-run
python3 scripts/verify_bonus9.py
```

فعال‌سازی کنترل‌کننده، پس از بررسی دستی بسته و تهیه نسخه پشتیبان:

```bash
install -m 644 deploy/distributed-banking-selfheal.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now distributed-banking-selfheal
systemctl status distributed-banking-selfheal --no-pager
journalctl -u distributed-banking-selfheal -n 30 --no-pager
```

به‌منظور اثبات بونوس، تست توقف یک Replica تحت خواندن هم‌زمان **فقط با تأیید وضعیت Controller و صفر بودن Prepared**:

```bash
python3 scripts/verify_bonus9.py --inject
```

برای تست خرابی خود Controller (بدون دست‌زدن به بانک): `sudo systemctl restart distributed-banking-selfheal` و سپس بررسی عدم Duplicate در `docker compose ps -a`. برای سناریوی دشوارتر، Controller را بین Replace و ثبت نتیجه در محیط تست جدا متوقف و مجدداً راه‌اندازی کنید. در صورت غیرفعال‌سازی کنترل‌کننده: `systemctl disable --now distributed-banking-selfheal`؛ کانتینرهای بانک حذف نمی‌شوند.

## بونوس ۱۰: طراحی عملیاتی مورد نیاز

**موانع فعلی کد:** `shard_map.py` دقیقاً دو شارد را می‌پذیرد؛ `storage.py` و `database/shard_b.sql` تعداد و محدوده B را ثابت فرض می‌کنند؛ `prepared_gid` فقط A/B را قبول می‌کند؛ `DecisionJournal` ستون‌های `a_ack/b_ack` دارد و مشارکت‌کنندگان تراکنش را بر اساس مالکیت تاریخی ذخیره نمی‌کند. `Gateway` و `Coordinator` نگاشت شارد را فقط در زمان راه‌اندازی بارگذاری می‌کنند. همه این‌ها پیش از مهاجرت آنلاین باید به شکل سازگار و نسخه‌دار بازنویسی و آزمایش شوند.

**طرح دقیق:**

1. C را با دو Replica و دیتابیس مجزا بسازید؛ ابتدا حسابی به آن منتقل نشود. Schema C و تغییرات B با Migration idempotent ایجاد شوند. Readiness به جای فرض ۱۰۰۰ حساب، مالکیت پایدار را کنترل کند.
2. یک رکورد Migration پایدار با `phase`, `source_epoch`, `target_epoch`, `snapshot_marker`, `last_applied_change`, `cutover_decision` در مخزن کنترل تراکنشی داشته باشید. وضعیت فقط رو به جلو تغییر کند.
3. تغییرات حساب‌های A1501..A2000 هنگام Snapshot و Copy همچنان پذیرفته شوند. از WAL/Logical Decoding یا یک CDC تراکنشی با **ترتیب Commit واقعی** استفاده شود؛ صرفاً `bigserial` بدون تضمین ترتیب Commit کافی نیست. Replay روی C باید از شناسه یکتا/Checkpoint پایدار و `UPSERT` شرطی استفاده کند؛ هیچ تغییر دوبار اعمال نشود.
4. برای هر Write، Epoch و صاحب‌حساب را در خود شارد در یک تراکنش با Row Lock/Ownership Lock اعتبارسنجی کنید؛ صرف بررسی در Gateway کافی نیست. درخواست قدیمی به B پس از Cutover باید به خطای `STALE_EPOCH` برسد.
5. Coordinator باید شرکت‌کنندگان ثابت و Epoch همان تراکنش را در Journal ذخیره کند و پس از تغییر Map همچنان Commit/Abort را به همان شرکت‌کنندگان تاریخی بفرستد. Transaction-ID برای Retry ثابت باشد و وضعیت نامعلوم با Status Query تعیین شود.
6. پیش از Cutover، مانع ایجاد Prepare جدید روی محدوده مهاجرت شوید؛ همه Preparedهای مرتبط و Commitهای durable را تعیین‌تکلیف کنید. در آخرین توقف کوتاه، B را Fence کنید، آخرین Delta را روی C اعمال و Check کنید، سپس Epoch مالکیت C را Activate و به Gateway/Coordinator منتشر کنید. ترتیب نوشتن و Fence نباید باعث دو Owner فعال شود.
7. پس از Cutover، حساب‌های B را فقط بعد از Cross-Check شمارش، Checksum، مجموع موجودی و Transaction Receipts، و بعد از عبور از Recovery Gates پاک کنید. هر مرحله Crash و Resume خودکار و قابل تکرار داشته باشد.

**تست‌های بونوس ۱۰:** (الف) Copy زیر بار concurrent Deposit/Withdraw و Transfer؛ (ب) Inject stale write به B و اثبات 409/STALE_EPOCH و ثبات موجودی؛ (پ) Prepare/Commit قبل و پس از Epoch همراه Crash Coordinator؛ (ت) Crash در چهار نقطه Snapshot/Replay/Fence/Post-Activation؛ (ث) ۵۰۰ حساب منتقل‌شده بدون Duplicate و حفظ Money Conservation.

## وضعیت واقعی این بسته

- **پیاده‌سازی شده:** Self-Healing Controller، تست‌های واحد آن، Read-Only Phase9 Demo، مدل محض Migration با قواعد Epoch/Checkpoint و پیش‌آزمون Read-Only Phase10؛ فایل سرویس systemd.
- **پیاده‌سازی نشده:** هیچ‌یک از مراحل انتقال واقعی داده، Shard C عملیاتی، CDC، Fence، 2PC سه‌شاردی و Recovery چهارگانه؛ این موارد به صورت صریح Gateهای مرحله بعدند.
- **هدف:** حفظ نتیجه ۷/۷ آزمایش و Image پایدار تا پایان همه الزامات Bonus.
