from app.generator.event_generator import EventGenerator


def test_generate_event():
    gen = EventGenerator()
    event = gen.generate_event()

    assert event.event_id is not None
    assert event.user_id.startswith("user_")