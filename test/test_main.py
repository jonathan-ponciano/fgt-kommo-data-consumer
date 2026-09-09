def test_example():
    """Teste simples de exemplo."""
    assert 1 + 1 == 2


def test_string():
    """Teste simples com strings."""
    result = "hello"
    assert result == "hello"
    assert len(result) == 5


def test_list():
    """Teste simples com listas."""
    my_list = [1, 2, 3]
    assert len(my_list) == 3
    assert 2 in my_list
