@echo off
REM Windows 定时任务配置脚本
REM 自动创建每天运行爬虫的任务

echo ============================================================
echo 青岛政府采购爬虫 - Windows 定时任务配置
echo ============================================================
echo.

REM 获取当前目录
set CURRENT_DIR=%CD%

REM 获取 Python 路径
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 未找到 Python，请先安装 Python 并添加到 PATH
    pause
    exit /b 1
)

for /f "delims=" %%i in ('where python') do set PYTHON_PATH=%%i
echo [信息] Python 路径：%PYTHON_PATH%

REM 删除旧任务（如果存在）
schtasks /delete /tn "青岛政府采购爬虫" /f >nul 2>&1

REM 创建新任务 - 每天早上 9:00 运行
echo.
echo [信息] 创建定时任务...
schtasks /create /tn "青岛政府采购爬虫" /tr "\"%PYTHON_PATH%\" \"%CURRENT_DIR%\main.py\"" /sc daily /st 09:00 /ru SYSTEM /f

if %errorlevel% equ 0 (
    echo.
    echo ============================================================
    echo [成功] 定时任务创建完成！
    echo ============================================================
    echo.
    echo 任务名称：青岛政府采购爬虫
    echo 运行时间：每天早上 9:00
    echo 项目路径：%CURRENT_DIR%
    echo.
    echo 查看任务：打开"任务计划程序" → "任务计划程序库"
    echo 手动运行：schtasks /run /tn "青岛政府采购爬虫"
    echo 删除任务：schtasks /delete /tn "青岛政府采购爬虫" /f
    echo.
) else (
    echo.
    echo [错误] 定时任务创建失败，请检查权限
    echo 可能需要以管理员身份运行此脚本
    echo.
)

pause
