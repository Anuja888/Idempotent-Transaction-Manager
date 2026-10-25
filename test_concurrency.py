import asyncio
import uuid
import httpx
import sys
import subprocess
import time
from decimal import Decimal

BASE_URL = "http://127.0.0.1:8000"

async def check_server_ready():
    """Wait for the FastAPI server to be responsive."""
    async with httpx.AsyncClient() as client:
        for _ in range(30):
            try:
                response = await client.get(f"{BASE_URL}/docs")
                if response.status_code == 200:
                    return True
            except httpx.RequestError:
                pass
            await asyncio.sleep(0.5)
    return False

async def seed_account(account_id: str, balance: Decimal):
    """Seed or update an account with a specific balance."""
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{BASE_URL}/accounts",
            json={"account_id": account_id, "balance": str(balance)}
        )
        assert response.status_code == 200, f"Failed to seed account: {response.text}"
        return response.json()

async def get_account_balance(account_id: str) -> Decimal:
    """Retrieve the current balance of an account."""
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{BASE_URL}/accounts/{account_id}")
        assert response.status_code == 200, f"Failed to get account: {response.text}"
        return Decimal(str(response.json()["balance"]))

async def send_charge(client: httpx.AsyncClient, idempotency_key: str, account_id: str, amount: Decimal):
    """Send a POST /charge request."""
    try:
        response = await client.post(
            f"{BASE_URL}/charge",
            json={
                "idempotency_key": idempotency_key,
                "account_id": account_id,
                "amount": str(amount)
            },
            timeout=15.0
        )
        return response.status_code, response.json()
    except Exception as e:
        return 500, {"error": str(e)}

async def run_test_1_concurrent_duplicates():
    """
    Test 1: Fire 100 concurrent duplicate requests with the same idempotency_key.
    Assert the account is charged exactly once and all requests get the same success response.
    """
    print("\n--- Running Test 1: 100 Concurrent Duplicate Requests ---")
    account_id = f"acc_dup_{uuid.uuid4().hex[:8]}"
    initial_balance = Decimal("100.00")
    charge_amount = Decimal("10.00")
    idempotency_key = str(uuid.uuid4())

    await seed_account(account_id, initial_balance)
    
    # Configure client with high limits for concurrency
    limits = httpx.Limits(max_keepalive_connections=150, max_connections=150)
    async with httpx.AsyncClient(limits=limits) as client:
        # Launch 100 concurrent requests
        tasks = [
            send_charge(client, idempotency_key, account_id, charge_amount)
            for _ in range(100)
        ]
        results = await asyncio.gather(*tasks)

    # Process results
    success_count = 0
    duplicate_count = 0
    fail_count = 0
    
    expected_response_body = None
    response_bodies_match = True

    for status_code, body in results:
        if status_code == 200:
            success_count += 1
            if expected_response_body is None:
                expected_response_body = body
            elif body != expected_response_body:
                response_bodies_match = False
        else:
            fail_count += 1

    final_balance = await get_account_balance(account_id)
    expected_balance = initial_balance - charge_amount

    # Assertions
    # 1. Account should be charged exactly once, so final balance is expected_balance
    balance_is_correct = (final_balance == expected_balance)
    # 2. All 100 requests should return 200 OK
    all_requests_succeeded = (success_count == 100)
    
    test_passed = balance_is_correct and all_requests_succeeded and response_bodies_match

    print(f"Initial Balance: ${initial_balance}")
    print(f"Charge Amount: ${charge_amount}")
    print(f"Final Balance: ${final_balance} (Expected: ${expected_balance})")
    print(f"Results: Total={len(results)}, Success (HTTP 200)={success_count}, Failed={fail_count}")
    print(f"Check - Balance Deducted Exactly Once: {'PASS' if balance_is_correct else 'FAIL'}")
    print(f"Check - All 100 Requests Succeeded: {'PASS' if all_requests_succeeded else 'FAIL'}")
    print(f"Check - Response Bodies Match: {'PASS' if response_bodies_match else 'FAIL'}")
    print(f"Overall Test 1 Result: {'PASS' if test_passed else 'FAIL'}")

    return {
        "passed": test_passed,
        "total": len(results),
        "success": success_count,
        "fail": fail_count,
        "balance_correct": balance_is_correct
    }

async def run_test_2_concurrent_different_charges():
    """
    Test 2: Fire 50 concurrent DIFFERENT charges against the same account.
    Assert the final balance is mathematically correct (no lost updates).
    """
    print("\n--- Running Test 2: 50 Concurrent Different Requests ---")
    account_id = f"acc_diff_{uuid.uuid4().hex[:8]}"
    initial_balance = Decimal("1000.00")
    charge_amount = Decimal("15.50")
    num_requests = 50

    await seed_account(account_id, initial_balance)
    
    # Configure client with high limits for concurrency
    limits = httpx.Limits(max_keepalive_connections=100, max_connections=100)
    async with httpx.AsyncClient(limits=limits) as client:
        # Launch 50 concurrent requests, each with a DIFFERENT idempotency key
        tasks = [
            send_charge(client, str(uuid.uuid4()), account_id, charge_amount)
            for _ in range(num_requests)
        ]
        results = await asyncio.gather(*tasks)

    # Process results
    success_count = 0
    fail_count = 0

    for status_code, body in results:
        if status_code == 200 and "Charge successful" in body.get("message", ""):
            success_count += 1
        else:
            fail_count += 1

    final_balance = await get_account_balance(account_id)
    expected_deduction = charge_amount * num_requests
    expected_balance = initial_balance - expected_deduction

    balance_is_correct = (final_balance == expected_balance)
    all_succeeded = (success_count == num_requests)
    
    test_passed = balance_is_correct and all_succeeded

    print(f"Initial Balance: ${initial_balance}")
    print(f"Charge Amount: ${charge_amount} x {num_requests} times")
    print(f"Total Expected Deduction: ${expected_deduction}")
    print(f"Final Balance: ${final_balance} (Expected: ${expected_balance})")
    print(f"Results: Total={len(results)}, Success (HTTP 200)={success_count}, Failed={fail_count}")
    print(f"Check - Balance is Mathematically Correct: {'PASS' if balance_is_correct else 'FAIL'}")
    print(f"Check - All 50 Requests Succeeded: {'PASS' if all_succeeded else 'FAIL'}")
    print(f"Overall Test 2 Result: {'PASS' if test_passed else 'FAIL'}")

    return {
        "passed": test_passed,
        "total": len(results),
        "success": success_count,
        "fail": fail_count,
        "balance_correct": balance_is_correct
    }

async def main():
    # Start the FastAPI server using uvicorn
    # Use sys.executable to run inside the current virtual environment
    cmd = [
        sys.executable, "-m", "uvicorn", 
        "main:app", 
        "--host", "127.0.0.1", 
        "--port", "8000", 
        "--log-level", "warning"
    ]
    print("Starting FastAPI server...")
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    try:
        # Wait for the server to be ready
        print("Waiting for server to start...")
        if not await check_server_ready():
            print("Error: FastAPI server failed to start.")
            return

        print("FastAPI server is running.")
        
        # Run Test 1
        t1_results = await run_test_1_concurrent_duplicates()
        
        # Run Test 2
        t2_results = await run_test_2_concurrent_different_charges()
        
        # Print final report summary
        print("\n" + "="*50)
        print("                 CONCURRENCY REPORT")
        print("="*50)
        print(f"TEST 1 (100 Concurrent Duplicate Requests):")
        print(f"  - Fired: {t1_results['total']}")
        print(f"  - HTTP 200 Successes: {t1_results['success']}")
        print(f"  - HTTP Failures: {t1_results['fail']}")
        print(f"  - Balance Correct (Charged Once): {'PASS' if t1_results['balance_correct'] else 'FAIL'}")
        print(f"  - Test Outcome: {'PASSED' if t1_results['passed'] else 'FAILED'}")
        print()
        print(f"TEST 2 (50 Concurrent Different Requests):")
        print(f"  - Fired: {t2_results['total']}")
        print(f"  - HTTP 200 Successes: {t2_results['success']}")
        print(f"  - HTTP Failures: {t2_results['fail']}")
        print(f"  - Balance Correct (No Lost Updates): {'PASS' if t2_results['balance_correct'] else 'FAIL'}")
        print(f"  - Test Outcome: {'PASSED' if t2_results['passed'] else 'FAILED'}")
        print("="*50)
        
        # Write results file for report generation
        with open("test_results.txt", "w") as f:
            f.write(f"T1_TOTAL={t1_results['total']}\n")
            f.write(f"T1_SUCCESS={t1_results['success']}\n")
            f.write(f"T1_FAIL={t1_results['fail']}\n")
            f.write(f"T1_PASSED={t1_results['passed']}\n")
            f.write(f"T2_TOTAL={t2_results['total']}\n")
            f.write(f"T2_SUCCESS={t2_results['success']}\n")
            f.write(f"T2_FAIL={t2_results['fail']}\n")
            f.write(f"T2_PASSED={t2_results['passed']}\n")

    except Exception as e:
        print(f"An error occurred during test execution: {e}")
    finally:
        print("Stopping FastAPI server...")
        proc.terminate()
        proc.wait()
        print("Server stopped.")

if __name__ == "__main__":
    asyncio.run(main())
