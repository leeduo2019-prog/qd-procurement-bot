@echo off
chcp 65001 >nul
REM 青岛政府采购爬虫 - Windows 快速部署脚本
REM 适用于 Windows 10/11

echo ============================================================
echo 青岛政府采购爬虫 - Windows 一键部署脚本
echo ============================================================
echo.

REM 检查 Python 是否安装
where python >nul 2>&1
if %errorlevel% neq 0 (
    echo [错误] 未检测到 Python，请先安装 Python 3.10+
    echo.
    echo 下载地址：https://www.python.org/downloads/
    echo 安装时请勾选 "Add Python to PATH"
    echo.
    pause
    exit /b 1
)

echo [信息] Python 已安装
python --version
echo.

REM 获取当前目录
set CURRENT_DIR=%CD%

REM 创建虚拟环境
echo ============================================================
echo 步骤 1: 创建 Python 虚拟环境
echo ============================================================
echo.

if exist venv (
    echo [信息] 虚拟环境已存在，跳过创建
) else (
    echo [信息] 创建虚拟环境...
    python -m venv venv
    if errorlevel 1 (
        echo [错误] 创建虚拟环境失败
        pause
        exit /b 1
    )
    echo [完成] 虚拟环境创建成功
)
echo.

REM 安装依赖
echo ============================================================
echo 步骤 2: 安装 Python 依赖
echo ============================================================
echo.

call venv\Scripts\activate.bat
python -m pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

if errorlevel 1 (
    echo [错误] 安装依赖失败
    pause
    exit /b 1
)
echo [完成] 依赖安装成功
echo.

REM 配置环境变量
echo ============================================================
echo 步骤 3: 配置环境变量
echo ============================================================
echo.

if exist .env (
    echo [信息] .env 文件已存在
) else (
    echo [信息] 创建 .env 配置文件...
    copy .env.example .env
    echo [完成] .env 文件已创建
    echo.
    echo [注意] 请编辑 .env 文件配置钉钉 Webhook 信息
    echo.
)
echo.

REM 测试运行
echo ============================================================
echo 步骤 4: 测试运行
echo ============================================================
echo.
echo 是否现在测试运行爬虫？(Y/N)
set /p RUN_TEST="> "
if /i "%RUN_TEST%"=="Y" (
    echo.
    echo [信息] 开始测试运行...
    python main.py
    echo.
    if errorlevel 1 (
        echo [警告] 测试运行失败，请检查配置
    ) else (
        echo [完成] 测试运行成功
    )
) else (
    echo [信息] 跳过测试运行
)
echo.

REM 配置定时任务
echo ============================================================
echo 步骤 5: 配置 Windows 定时任务
echo ============================================================
echo.
echo 是否配置定时任务？(Y/N)
echo 配置后将在每天早上 9:00 自动运行
set /p CONFIG_TASK="> "
if /i "%CONFIG_TASK%"=="Y" (
    echo.
    
    REM 获取 Python 完整路径
    for %%i in (python.exe) do set PYTHON_FULL_PATH=%%~dpfi
    echo [信息] Python 路径：%PYTHON_FULL_PATH%
    echo.
    
    REM 删除旧任务
    schtasks /delete /tn "青岛政府采购爬虫" /f >nul 2>&1
    
    REM 创建新任务
    echo [信息] 创建定时任务...
    schtasks /create /tn "青岛政府采购爬虫" /tr "\"%PYTHON_FULL_PATH%\" \"%CURRENT_DIR%\main.py\"" /sc daily /st 09:00 /ru SYSTEM /f
    
    if errorlevel 0 (
        echo [完成] 定时任务创建成功
        echo.
        echo 任务名称：青岛政府采购爬虫
        echo 运行时间：每天早上 9:00
        echo.
        echo 管理命令:
        echo   查看任务：taskschd.msc
        echo   手动运行：schtasks /run /tn "青岛政府采购爬虫"
        echo   删除任务：schtasks /delete /tn "青岛政府采购爬虫" /f
    ) else (
        echo [错误] 定时任务创建失败
        echo 请右键此脚本，选择"以管理员身份运行"
    )
) else (
    echo [信息] 跳过定时任务配置
    echo.
    echo 提示：可以稍后运行 setup_windows_task.bat 配置定时任务
)
echo.

REM 完成
echo ============================================================
echo 部署完成！
echo ============================================================
echo.
echo 项目路径：%CURRENT_DIR%
echo 配置文件：%CURRENT_DIR%\.env
echo.
echo 下一步:
echo 1. 编辑 .env 文件，配置钉钉 Webhook 信息
echo 2. 运行 python main.py 测试
echo 3. 如未配置定时任务，可运行 setup_windows_task.bat
echo.
echo 使用说明:
echo   手动运行：python main.py
echo   查看日志：type crawler.log
echo.

pause
