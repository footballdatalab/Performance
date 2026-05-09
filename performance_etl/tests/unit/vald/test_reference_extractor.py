from __future__ import annotations

from ingestion.vald.extractors.reference_extractor import ValdReferenceExtractor


class _FakeEndpoint:
    def __init__(self, athletes_by_tenant: dict[str, list[dict]]) -> None:
        self.athletes_by_tenant = athletes_by_tenant

    def get_athletes(self, tenant_id: str) -> list[dict]:
        return self.athletes_by_tenant.get(tenant_id, [])


class _FakeRawLoader:
    def __init__(self, inserted_tenants: set[str]) -> None:
        self.inserted_tenants = inserted_tenants
        self.calls: list[tuple[str, dict[str, str]]] = []

    def load_raw_if_changed_with_status(self, **kwargs):
        self.calls.append((kwargs["table_name"], kwargs["request_params"]))
        tenant_id = kwargs["request_params"]["teamId"]
        inserted = tenant_id in self.inserted_tenants
        return (100 if inserted else 42, inserted)


def test_reference_extractor_tracks_written_snapshots() -> None:
    class _FakeValdClient:
        forcedecks_client = object()

    extractor = ValdReferenceExtractor(
        vald_client=_FakeValdClient(),
        raw_loader=_FakeRawLoader(inserted_tenants={"tenant-1"}),
        batch_manager=object(),
    )
    extractor._fd_ep = _FakeEndpoint(
        {
            "tenant-1": [{"id": "p1"}, {"id": "p2"}],
            "tenant-2": [{"id": "p3"}],
        }
    )

    summary = extractor.extract_all(["tenant-1", "tenant-2"])

    assert summary["profiles_seen"] == 3
    assert summary["snapshots_written"] == 1
    assert summary["errors"] == []
