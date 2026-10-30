import json
import subprocess
import time
import urllib.request
import urllib.error

BASE_URL = "http://localhost:8080"

def get_json(url):
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode('utf-8'))

def post_json(url, data):
    body = json.dumps(data).encode('utf-8')
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode('utf-8'))

def run_cmd(cmd):
    return subprocess.run(cmd, shell=True, capture_output=True, text=True)

def main():
    print("=" * 60)
    print("[CHAOS-MONKEY] Starting Crash-Proof Durability Stress Test")
    print("=" * 60)

    # 1. Initial stock
    products = get_json(f"{BASE_URL}/api/workflows/products")
    iphone = next((p for p in products if p['sku'] == 'SKU-IPHONE'), None)
    if not iphone:
        print("ERROR: SKU-IPHONE not found in catalog!")
        return
    initial_stock = iphone['stockQuantity']
    print(f"[1] Initial stock for SKU-IPHONE: {initial_stock}")

    # 2. Trigger workflow with simulated crash
    payload = {
        "sku": "SKU-IPHONE",
        "quantity": 1,
        "customerName": "Chaos Monkey Tester",
        "customerEmail": "chaos@flowlab.dev",
        "simulate_crash": True,
        "simulate_fatal": False,
        "simulate_fraud": False
    }
    exec_data = post_json(f"{BASE_URL}/api/workflows/order-processing-workflow/start", payload)
    exec_id = exec_data['id']
    print(f"[2] Triggered workflow execution: {exec_id}")

    # 3. Wait for mid-flight execution, then kill the container!
    print("[3] Waiting 1.5s for step execution to begin...")
    time.sleep(1.5)

    print("[4] KILLING container: docker kill flowlab-engine...")
    run_cmd("docker kill flowlab-engine")
    print("[4] Container is DEAD. Verifying down state...")
    time.sleep(3)

    # 4. Revive container
    print("[5] REVIVING container: docker start flowlab-engine...")
    run_cmd("docker start flowlab-engine")
    print("[5] Container started. Waiting 12s for Spring Boot and CrashRecoveryService...")
    time.sleep(12)

    # 5. Poll for recovery completion
    print("[6] Monitoring self-healing recovery...")
    final_status = ""
    for second in range(0, 40, 2):
        try:
            status_data = get_json(f"{BASE_URL}/api/workflows/executions/{exec_id}")
            final_status = status_data['status']
            curr_step = status_data['currentStepIndex']
            print(f"    +{second}s status: {final_status} (Step #{curr_step})")
            if final_status in ["COMPLETED", "FAILED", "COMPENSATED"]:
                break
        except Exception:
            print(f"    +{second}s engine rebooting...")
        time.sleep(2)

    # 6. Verify assertions
    print("\n" + "=" * 60)
    print("CHAOS MONKEY VERIFICATION SUMMARY")
    print("=" * 60)

    try:
        steps = get_json(f"{BASE_URL}/api/workflows/executions/{exec_id}/steps")
        for s in steps:
            print(f"  Step #{s['stepIndex']} [{s['stepName']}]: status={s['status']}, attempts={s['attemptCount']}")
    except Exception as e:
        print(f"Could not fetch steps: {e}")

    updated_products = get_json(f"{BASE_URL}/api/workflows/products")
    updated_iphone = next((p for p in updated_products if p['sku'] == 'SKU-IPHONE'), None)
    updated_stock = updated_iphone['stockQuantity'] if updated_iphone else None

    print(f"Stock Before: {initial_stock} | Stock After: {updated_stock}")

    if final_status == "COMPLETED" and updated_stock == initial_stock - 1:
        print("\n>>> ALL ASSERTIONS PASSED! <<<")
        print("The engine survived container kill, self-healed, and resumed safely with ZERO duplicate debit!")
    else:
        print(f"\nAssertion mismatch: final_status={final_status}, expected_stock={initial_stock - 1}, actual={updated_stock}")

if __name__ == '__main__':
    main()
