import pytest

@pytest.mark.asyncio # اجرای متد به صورت غیر همزمان
async def test_create_app(client):
 
    response = await client.post(
        "/api/users/create/",
        json={
            "name": "Test User",
            "email": "test@example.com",
        },
    )

    assert response.status_code == 201

    data = response.json()

    assert data["name"] == "Test User"
    assert data["email"] == "test@example.com"
    assert isinstance(data["id"], int)

@pytest.mark.asyncio
async def test_create_user_duplicate_email(client):
    
    first_response = await client.post(
        "/api/users/create/",
        json={
            "name": "First User",
            "email": "duplicate@example.com",
        },
    )

    second_response = await client.post(
        "/api/users/create/",
        json={
            "name": "Second User",
            "email": "duplicate@example.com",
        },
    )

    assert first_response.status_code == 201
    assert second_response.status_code == 400
    assert second_response.json()["detail"] == "Email already exists"

@pytest.mark.asyncio
async def test_create_user_with_null_name(client):
    response = await client.post(
        "/api/users/create/",
        json={
            "name":None,
            "email":"test@example.com"
        }
    )

    assert response.status_code == 422

    error_text = response.text.lower()
    assert "name" in error_text
    assert "string" in error_text or "null" in error_text

@pytest.mark.asyncio
async def test_create_user_with_null_email(client):
    response = await client.post(
        "/api/users/create/",
        json={
            "name":"test",
            "email":None
        }
    )
    
    assert response.status_code == 422
    
    error_text = response.text.lower()
    assert "email" in error_text
    assert "string" in error_text or "null" in error_text

@pytest.mark.asyncio
async def test_create_user_with_invalid_email(client):
    response = await client.post(
        '/api/users/create/',
        json={
            "name":"Test User",
            "email":"not-an-email"
        }
    )

    assert response.status_code == 422

    error_text = response.text.lower()
    assert "email" in error_text

@pytest.mark.asyncio
async def test_create_user_with_long_name(client):
    response = await client.post(
        '/api/users/create/',
        json={
            "name": "a" * 65,
            "email": "longname@example.com",
        }
    )

    assert response.status_code == 422

    error_text = response.text.lower()
    assert "name" in error_text

@pytest.mark.asyncio
async def test_get_users(client):
    await client.post(
        "/api/users/create/",
        json={
            "name":"First User",
            "email":"first@example.com"
        }
    )

    await client.post(
        '/api/users/create/',
        json={
            "name":"Second User",
            "email":"second@example.com"
        }
    )

    response = await client.get(
        '/api/users/'
    )

    assert response.status_code == 200

    data = response.json()

    assert len(data) == 2

    assert data[0]["name"] == "First User"
    assert data[0]["email"] == "first@example.com"

    assert data[1]["name"] == "Second User"
    assert data[1]["email"] == "second@example.com"

    assert data[0]["id"] < data[1]["id"]

@pytest.mark.asyncio
async def test_get_user_response_200(client):
    create_response = await client.post(
        "/api/users/create/",
        json={
            "name":"Single User",
            "email":"single@example.com"
        }
    )

    user_id = create_response.json()["id"]

    response = await client.get(
        f"/api/users/{user_id}/"
    )

    assert response.status_code == 200

    data = response.json()

    assert data["id"] == user_id
    assert data["name"] == "Single User"
    assert data["email"] == "single@example.com"

@pytest.mark.asyncio
async def test_get_user_not_found(client):
    response = await client.get(
        '/api/users/999999/'
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "User not found"

@pytest.mark.asyncio
async def test_update_user(client):
    create_response = await client.post(
        '/api/users/create/',
        json={
            "name": "Old Name",
            "email":"old@example.com"
        }
    )

    user_id = create_response.json()["id"]

    response = await client.patch(
        f"/api/users/{user_id}/",
        json={
            "name":"New Name",
            "email":"new@example.com"
        }
    )

    assert response.status_code == 200

    data = response.json()

    assert data["name"] == "New Name"
    assert data["email"] == "new@example.com"

@pytest.mark.asyncio
async def test_update_user_with_duplicate_email(client):
    first_response = await client.post(
        "/api/users/create/",
        json={
            "name":"First Name",
            "email":"first@example.com"
        }
    )

    second_response = await client.post(
        "/api/users/create/",
        json={
            "name":"Second Name",
            "email":"seconde@example.com"
        }
    )

    first_user_id = first_response.json()["id"]
    second_user_id = second_response.json()["id"]

    assert first_user_id != second_user_id

    response = await client.patch(
        f"/api/users/{second_user_id}/",
        json={
            "name":"Update Seconde name",
            "email":"first@example.com"
        }
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Email already exists"

@pytest.mark.asyncio
async def test_update_user_not_found(client):
    response = await client.patch(
        '/api/users/99999/',
        json={
            "name":"Single User",
            "email":"single@example.com"
        }
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "User not found"

@pytest.mark.asyncio
async def test_delete_user(client):
    create_response = await client.post(
        '/api/users/create/',
        json={
            "name":"Test User",
            "email":"test@example.com"
        }
    )

    user_id = create_response.json()["id"]

    response = await client.delete(
        f'/api/users/{user_id}/'
    )

    assert response.status_code == 200
    assert response.json()["message"] == "user removed successfully"

@pytest.mark.asyncio
async def test_delete_user_not_found(client):
    response = await client.delete(
        '/api/users/999999/'
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "User not found"  