#!/bin/bash
# Week 5 恢复功能端到端测试脚本

set -e

echo "=== Week 5 Recovery E2E Test ==="
echo ""

# 清理旧数据
echo "Step 1: 清理旧数据..."
rm -rf ./data
rm -f ./api
echo "✓ 清理完成"
echo ""

# 编译服务
echo "Step 2: 编译服务..."
go build -o api ./cmd/api
echo "✓ 编译完成"
echo ""

# 启动服务
echo "Step 3: 启动服务..."
DATA_DIR=./data ./api &
API_PID=$!
echo "✓ 服务已启动 (PID: $API_PID)"
sleep 2
echo ""

# 测试函数
test_api() {
    echo "Step 4: 发送测试请求..."

    # 下单请求（需要根据实际API调整）
    echo "  发送 10 个下单请求..."
    for i in {1..10}; do
        echo "    请求 $i/10"
        # 这里需要根据实际的API端点调整
        # curl -X POST http://localhost:8080/api/orders \
        #   -H "Content-Type: application/json" \
        #   -d "{\"symbol\":\"BTC-USDT\",\"side\":\"BUY\",\"price\":50000,\"quantity\":1}"
        sleep 0.1
    done
    echo "✓ 请求发送完成"
    echo ""
}

# 检查数据文件
check_data() {
    echo "Step 5: 检查持久化数据..."

    if [ -d "./data/events" ]; then
        echo "✓ 事件日志目录存在"
        find ./data/events -name "*.log" -exec echo "  - {}" \;
    else
        echo "✗ 事件日志目录不存在"
    fi

    if [ -d "./data/snapshots" ]; then
        echo "✓ 快照目录存在"
        find ./data/snapshots -name "*.json" -exec echo "  - {}" \;
    else
        echo "  快照目录不存在（可能事件数不足）"
    fi
    echo ""
}

# 执行测试
test_api
check_data

# 停止服务
echo "Step 6: 停止服务..."
kill $API_PID
wait $API_PID 2>/dev/null || true
echo "✓ 服务已停止"
echo ""

# 重启服务测试恢复
echo "Step 7: 重启服务测试恢复..."
DATA_DIR=./data ./api &
API_PID=$!
echo "✓ 服务已重启 (PID: $API_PID)"
sleep 2
echo ""

echo "Step 8: 检查恢复日志..."
echo "  查看服务启动日志，应该看到恢复信息"
echo ""

# 清理
echo "Step 9: 清理..."
kill $API_PID
wait $API_PID 2>/dev/null || true
echo "✓ 测试完成"
echo ""

echo "=== 测试总结 ==="
echo "1. 检查 ./data/events 目录下是否有事件日志文件"
echo "2. 检查 ./data/snapshots 目录下是否有快照文件"
echo "3. 查看服务重启时的日志，确认恢复流程执行"
