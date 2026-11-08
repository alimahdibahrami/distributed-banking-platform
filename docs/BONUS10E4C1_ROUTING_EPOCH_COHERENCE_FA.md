# فاز 10E-4C1 — همخوانی نسخه مسیریابی Gateway و Coordinator

## مرز دامنه

این تغییر یک محافظ عملی روی APIهای واقعی Gateway و Coordinator است؛ برخلاف هسته Control Plane، در مسیر رسیدگی به درخواست بانکی فراخوانی می‌شود. با این حال **هیچ پایگاه داده‌ای را مهاجرت نمی‌دهد، نقشه زنده را منتشر نمی‌کند، حساب‌ها را انتقال نمی‌دهد و مجوز Cutover نیست**.

مبنای مورد انتظار: Branch `feature/bonus10e-source-foundation`، Commit `8a0a0bf`، بدون تغییر در `main`.

## رفع خطر

به‌روزرسانی Gateway و Coordinator ناهمزمان است. اگر Gateway هنوز Version=1 و Coordinator نسخه=2 داشته باشد، درخواست جدید نباید تحت دو برداشت متفاوت از مالکیت حساب اجرا شود. با فعال‌بودن گیت، Gateway در مسیرهای GET Balance، POST Balance، و تمام عملیات مالی، قبل از هدایت درخواست، وضعیت `/health` هماهنگ‌کننده را بررسی می‌کند. شرط پذیرش: HTTP 200، `status=alive`، `service=coordinator`، `transactions_ready=true` و `routing_epoch == shard_map.version` با نوع `int` واقعی.

اگر هماهنگ‌کننده در دسترس نباشد یا نسخه ناهماهنگ باشد، **503 / ROUTING_EPOCH_UNAVAILABLE** برمی‌گردد. Gateway نسخه انتخاب‌شده را با `owner_epoch` به Coordinator می‌فرستد. Coordinator پیش از ثبت تراکنش جدید، Epoch را اعتبارسنجی می‌کند و در اختلاف **409 / ROUTING_EPOCH_MISMATCH** می‌دهد. درخواست عمومی اجازه تعیین `owner_epoch` ندارد و تراکنش‌های تاریخی همچنان از مسیر GET Status قابل بررسی‌اند.

## فعال‌سازی فقط در محیط سه‌شاردی مجزا

هر دو متغیر زیر باید **همزمان** روی Gateway و Coordinator فعال باشند:

```dotenv
BANK_EPOCH_PROPAGATION=1
BANK_REQUIRE_ROUTING_EPOCH=1
```

برای شاردها `BANK_EPOCH_PROPAGATION=1` لازم است، اما کافی نیست: Trigger پایدار SQL و دیتابیس هم باید آماده باشند. اگر فقط `BANK_REQUIRE_ROUTING_EPOCH=1` باشد، سرویس در هنگام راه‌اندازی خطا می‌دهد، نه اینکه محافظت را دور بزند. به‌صورت پیش‌فرض این ویژگی خاموش است تا آزمایش‌های Assignment 2 و استقرار A/B قبلی تغییر نکنند.

## اثبات انجام‌شده

- ۲۰۰ تست پیشین پروژه + ۲۷ تست جدید = ۲۲۷ PASS (Unit/Contract، اجرای محلی).
- آزمون‌های تطابق Epoch=2، تفاوت Epoch=1/3، قطع شبکه، False بودن readiness، جعل فیلد عمومی، عملیات B↔C، عدم ثبت تراکنش با Epoch قدیمی و وضعیت تراکنش‌های تاریخی.
- هنوز آزمون عملی Runtime جدید با PostgreSQL و هر سه Replica در قالب واحد انجام نشده است.

## پیش‌شرط‌های Cutover که این بسته انجام نمی‌دهد

1. آماده‌سازی سه دیتابیس و دو Replica برای C؛ Identity جدید B باید با تعداد حساب‌ها هماهنگ باشد.
2. مانع نوشتن پایدار در DB با Trigger و اثبات بسته‌شدن هر Prepared 2PC؛ تنها بررسی API کافی نیست.
3. همگرایی Snapshot و CDC با Snapshot Digest و ثبت Evidence در Control Plane.
4. انتشار اتمیک Epoch جدید و راه‌اندازی هماهنگ سرویس‌ها، بدون پذیرش درخواست با Map کهنه.
5. اجرای واقعی آزمون‌های خرابی و بازگشت عقب با داده‌های ایزوله؛ تغییر روی B زنده هنوز ممنوع است.

تا تکمیل این موارد، `cutover_authorized=false` باقی می‌ماند. از اجرای SQL روی سرویس اصلی و تغییر `compose.yaml` خودداری کنید.
