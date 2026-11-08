# فاز ۱۰E-4C2 — گیت سازگاری Replica و هویت فیزیکی مالک (Shard Cohort)

## هدف و دامنه

این مرحله **مسیر خواندن، انتخاب Replica و آماده‌سازی 2PC را به اثبات مالکیت دیتابیس** پیوند می‌دهد. این مرحله Cutover خودکار، انتقال حساب‌های عملیاتی و انتشار نقشه جدید انجام نمی‌دهد. تمام کنترل‌های جدید به‌صورت پیش‌فرض خاموش‌اند.

مبنای تغییرات: Branch `feature/bonus10e-source-foundation`، Commit `524ccef`.

## قابلیت‌های این بسته

- `bank_platform/migration/shard_cohort.py`: قرارداد شناسایی Epoch و Replica، جلوگیری از پذیرش گواهی ناقص یا نامعتبر در `/ready`.
- `gateway/routing.py`: در حالت سخت‌گیرانه، Replica بدون تایید دیتابیس کنار گذاشته می‌شود؛ درخواست GET موجودی با `owner_epoch` ارسال می‌شود.
- `coordinator/engine.py`: 2PC روی Replica با Epoch متفاوت شروع نمی‌شود.
- `shard/storage.py`: مالکیتِ خواندن حساب‌های A1501..A2000 با `FOR SHARE` روی ردیف Fence بررسی می‌شود. این Lock اجازه نمی‌دهد Cutover هم‌زمان از کنار یک خواندن عبور کند. نوشتن بدون Epoch در حالت سخت‌گیرانه پذیرفته نیست.
- `shard/app.py`: فقط در حالت فعال، `/ready` شامل `ownership_verified` و `routing_epoch` می‌شود؛ وضعیت از PostgreSQL گرفته می‌شود و از روی فایل JSON حدس زده نمی‌شود.
- `database/bonus10e4c2_physical_owner_guard.sql`: نسخه تقویت‌شده Trigger نوشتن؛ اگر مالک تعیین‌شده در جدول Fence با `shard_metadata.shard_id` ناسازگار باشد، درخواست نوشتن به محدوده مهاجرت رد می‌شود، حتی اگر کلاینت مقدار جعلی Epoch/Owner ارائه کند. شارد C در حالت Standby اجازه نوشتن ندارد.

## تنظیم‌های اجرای اختیاری (برای آینده، نه استقرار فعلی)

در تمام Gateway، Coordinator و Replicaهای یک توپولوژی *کاملا مهاجرت‌یافته*:

```
BANK_EPOCH_PROPAGATION=1
BANK_REQUIRE_ROUTING_EPOCH=1
BANK_REQUIRE_SHARD_COHORT=1
```

در Replicaها به‌صورت مجزا، `BANK_ROUTING_EPOCH=2` و برای C نیز `BANK_ENABLE_SHARD_C=1` لازم است. این تنظیم‌ها **نباید روی سامانه فعلی دوشاردی VPS اعمال شوند**؛ پیش از تکمیل انتقال داده و هویت جدول‌های B/C، Ready به‌درستی 503 خواهد شد.

در صورت خاموش بودن `BANK_REQUIRE_SHARD_COHORT`، مسیرهای قدیمی بدون تغییر اجرا می‌شوند. در حالت روشن، کل سامانه باید هم‌نسخه و آماده باشد: فایل نقشه، Coordinator، Gateway، کد Replicaها، `shard_metadata` و `bank_migration_fence`.

## پروتکل عملی باقی‌مانده

1. راه‌اندازی PostgreSQL و C عملیاتی در توپولوژی مستقل و قابل بازیابی.
2. نصب و آزمون Triggerهای ۱۰E-2 و ۱۰E-4C2 در دو دیتابیس آزمایشی B/C؛ تست مستقیم Spoofed Epoch و نوشتن روی C Standby.
3. Snapshot و CDC زنده؛ قفل‌کردن B، تخلیه Preparedها و مطابقت Digest.
4. فعال‌سازی ۵۰۰ حساب روی جدول `accounts` واقعی C، به‌روزرسانی `shard_metadata` و Fence با ثبت انتقال پایدار و قابلیت Resume.
5. انتشار نسخه ۲ نقشه هماهنگ Gateway/Coordinator/Replicaها، فعال‌سازی سخت‌گیری و تست خواندن و 2PC.
6. فقط پس از تأیید یکپارچه، پاک‌سازی حساب‌های قدیمی B با مکانیزم اختصاصیِ کنترل‌شده (Trigger جدید عمدا حذف مستقیم این حساب‌ها را ممنوع می‌کند).

**هشدار:** هیچ فایل این بسته مجوز اجرای دستورات مهاجرت روی پایگاه داده اصلی نیست. وضعیت ۱۰E-4C2 = «سورس و تست قراردادی»؛ وضعیت Cutover = «هنوز اجرا نشده».

## پوشش آزمون

`tests/test_bonus10e4c2_shard_cohort.py` حالت‌های بد Epoch، گواهی Replica، read-after-fence، C standby، گزینش Gateway، 2PC و قرارداد SQL را می‌سنجد. اجرای این آزمون‌های Python جای آزمون واقعی Lock/Trigger PostgreSQL را نمی‌گیرد.
