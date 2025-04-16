async def test_ping_no_param(app_client):
    """
    Test ping endpoint with a mocked client.
    Args:
        app_client (TestClient): mocked client fixture
    """
    res = await app_client.get("/_mgmt/ping")
    assert res.status_code == 200
    response_json = res.json()
    assert response_json["message"] == "PONG"
    assert "database" in response_json
    assert response_json["database"] == "OK"
