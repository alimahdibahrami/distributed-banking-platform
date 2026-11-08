# ۱۰E-5 — آزمایشگاه مستقل تزریق خرابی و بازیابی مهاجرت

مبنای سورس: `feature/bonus10e-source-foundation`، Commit `f50ccbc` با Hotfixهای اثبات‌شده در ۱۰E-4D.

این پروژه Docker Compose انحصاری با نام `distributed-banking-b10e5-crash`، پایگاه‌های داده مستقل `b10e5_a/b/c` و پورت `127.0.0.1:18082` دارد. نه سامانه اصلی `8001` و نه آزمایشگاه موفق `18081` تغییر نمی‌کنند. از Image موجود استفاده می‌شود و هیچ دستور `down -v` یا `prune` در فرآیند اجرا نیست.

## ترتیب اجرا در VPS

1. `sha256sum -c SHA256SUMS.txt`
2. `bash scripts/preflight_lab.sh`
3. `bash scripts/start_lab.sh`
4. فقط بعد از PASS مرحله ۳: `bash scripts/crash_matrix.sh 2>&1 | tee /root/bonus10e5-crash-matrix.log`

## هدف آزمون

پنج خرابی عمدی بعد از Commit Capture، بعد از Snapshot، بعد از Fence، بعد از فعال‌سازی C و بعد از تغییر فایل‌های نقشه/تنظیمات ولی پیش از Restart سرویس‌ها. خروج با کد ۸۶ باید رخ دهد. پس از هر خرابی، همان گام از Journal بازیابی می‌شود. ۸۰ تراکنش فقط یک بار اجرا می‌شوند. در مرحله انتشار، سلامت و خواندن حساب A0500 در طول rollout اندازه گرفته می‌شود. POST-2PC فقط یک بار بعد از Verify انجام می‌شود.

خروجی قابل انتظار: `BONUS10E5_INTEGRATED_CRASH_RESUME_MATRIX_PASS` و `BONUS10E5_POST_CRASH_FINAL_INVARIANTS_PASS`، همراه `publish_availability.json`.

**محدودیت:** این آزمایشگاه Zero-Downtime را اثبات نمی‌کند؛ انتشار هنوز Controlled Cutover است. اگر هر گام شکست خورد، `crash_matrix.sh` را دوباره اجرا نکنید؛ فقط `bash scripts/operator.sh status` و لاگ را گزارش کنید. برای آزادسازی RAM فقط کانتینرهای پروژه را STOP کنید و Volumeها را نگه دارید.
