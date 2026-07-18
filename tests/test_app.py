from unittest.mock import Mock

import app


def test_trigger_rejects_invalid_secret(monkeypatch):
    monkeypatch.setattr(app, "WORKFLOW_SECRET", "correct")
    monkeypatch.setattr(app, "GH_API_TOKEN", "token")
    client = app.app.test_client()

    response = client.post("/api/trigger", json={"key": "wrong"})

    assert response.status_code == 401


def test_trigger_dispatches_manual_workflow(monkeypatch):
    monkeypatch.setattr(app, "WORKFLOW_SECRET", "correct")
    monkeypatch.setattr(app, "GH_API_TOKEN", "token")
    github_response = Mock(status_code=204)
    post = Mock(return_value=github_response)
    monkeypatch.setattr(app.requests, "post", post)
    client = app.app.test_client()

    response = client.post("/api/trigger", json={"key": "correct"})

    assert response.status_code == 200
    assert post.call_args.kwargs["json"] == {"ref": "main"}
