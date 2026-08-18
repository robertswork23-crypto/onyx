"""API over stored credential capability reports.

The blocking-validation recorder and the granular check-runner task write the
rows; these endpoints read them and trigger runs. Reports are advisory:
nothing here gates connector creation or indexing, and check failures are
report content, never an HTTP error.
"""

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from onyx.auth.permissions import require_permission
from onyx.background.celery.versioned_apps.client import app as client_app
from onyx.configs.constants import (
    DocumentSource,
    OnyxCeleryPriority,
    OnyxCeleryQueues,
    OnyxCeleryTask,
)
from onyx.connectors.capability_checks.models import CredentialCapabilityReport
from onyx.connectors.capability_checks.runner import (
    capability_check_run_ceiling_seconds,
    capability_check_run_stale_after,
)
from onyx.db.connector import fetch_connector_by_id
from onyx.db.credential_capability import (
    get_capability_report_row,
    get_capability_report_rows_for_source,
    mark_capability_report_running,
)
from onyx.db.credentials import (
    fetch_credential_by_id_for_user,
    fetch_credentials_by_source_for_user,
)
from onyx.db.engine.sql_engine import get_session
from onyx.db.enums import CapabilityCheckTrigger, CapabilityReportRunStatus, Permission
from onyx.db.models import CredentialCapabilityReportRow, User
from onyx.error_handling.error_codes import OnyxErrorCode
from onyx.error_handling.exceptions import OnyxError
from shared_configs.contextvars import get_current_tenant_id

router = APIRouter(prefix="/manage")


class CapabilityReportSnapshot(BaseModel):
    """One stored report row; ``report`` is the last completed run's content."""

    credential_id: int
    connector_id: int | None
    source: DocumentSource
    trigger: CapabilityCheckTrigger
    run_status: CapabilityReportRunStatus
    run_started_at: datetime | None
    connector_config_hash: str | None
    report: CredentialCapabilityReport | None
    time_updated: datetime

    @classmethod
    def from_row(cls, row: CredentialCapabilityReportRow) -> "CapabilityReportSnapshot":
        return cls(
            credential_id=row.credential_id,
            connector_id=row.connector_id,
            source=row.source,
            trigger=row.trigger,
            run_status=row.run_status,
            run_started_at=row.run_started_at,
            connector_config_hash=row.connector_config_hash,
            report=(
                CredentialCapabilityReport.model_validate(row.report)
                if row.report is not None
                else None
            ),
            time_updated=row.time_updated,
        )


class CapabilityCheckRunRequest(BaseModel):
    """Body of the trigger endpoint: which scope to run against.

    Both fields absent is the config-less credential-scoped run.
    ``connector_specific_config`` overrides the connector's stored config (a
    not-yet-saved edit) and is only meaningful with ``connector_id``.
    """

    connector_id: int | None = None
    connector_specific_config: dict[str, Any] | None = None


@router.post("/admin/credential/{credential_id}/capability-check")
def trigger_capability_check(
    credential_id: int,
    request: CapabilityCheckRunRequest,
    user: User = Depends(
        require_permission(Permission.MANAGE_CONNECTORS, allow_scope=True)
    ),
    db_session: Session = Depends(get_session),
) -> CapabilityReportSnapshot:
    """Marks the scope RUNNING, enqueues the check run, and returns the row.

    Accepted-style: the run happens on a worker and the caller polls the GET;
    the previous report stays readable meanwhile. A run already RUNNING within
    the staleness bound makes this a no-op returning the standing row.
    """
    # GATE 2 for ``allow_scope``: the user-filtered fetch is the visibility
    # check, and an unknown credential is indistinguishable from an
    # inaccessible one.
    credential = fetch_credential_by_id_for_user(credential_id, user, db_session)
    if credential is None:
        raise OnyxError(
            OnyxErrorCode.CREDENTIAL_NOT_FOUND,
            f"Credential {credential_id} does not exist or is not accessible.",
        )
    if request.connector_specific_config is not None and request.connector_id is None:
        raise OnyxError(
            OnyxErrorCode.INVALID_INPUT,
            "connector_specific_config requires connector_id: the "
            "credential-scoped run is config-less by definition.",
        )
    if request.connector_id is not None:
        connector = fetch_connector_by_id(request.connector_id, db_session)
        if connector is None:
            raise OnyxError(
                OnyxErrorCode.CONNECTOR_NOT_FOUND,
                f"Connector {request.connector_id} does not exist.",
            )
        if connector.source != credential.source:
            raise OnyxError(
                OnyxErrorCode.INVALID_INPUT,
                f"Connector {request.connector_id} is a "
                f"{connector.source.value} connector; credential "
                f"{credential_id} is for {credential.source.value}.",
            )
    row = mark_capability_report_running(
        db_session,
        credential_id=credential_id,
        connector_id=request.connector_id,
        source=credential.source,
        trigger=CapabilityCheckTrigger.MANUAL,
        active_within=capability_check_run_stale_after(credential.source),
    )
    if row is None:
        # An unexpired run is in flight; return its row without re-enqueueing.
        standing = get_capability_report_row(
            db_session, credential_id, request.connector_id
        )
        assert standing is not None, "The mark is only blocked by an existing row."
        return CapabilityReportSnapshot.from_row(standing)
    snapshot = CapabilityReportSnapshot.from_row(row)
    # Commit before enqueueing so the worker can only observe the RUNNING mark.
    db_session.commit()
    client_app.send_task(
        OnyxCeleryTask.RUN_CAPABILITY_CHECKS,
        kwargs=dict(
            credential_id=credential_id,
            connector_id=request.connector_id,
            connector_specific_config=request.connector_specific_config,
            tenant_id=get_current_tenant_id(),
        ),
        queue=OnyxCeleryQueues.CAPABILITY_CHECKS,
        priority=OnyxCeleryPriority.HIGH,
        # Queue wait is bounded by one execution ceiling; the staleness cutoff
        # above allows for both, so an expired task never strands the scope.
        expires=capability_check_run_ceiling_seconds(credential.source),
    )
    return snapshot


@router.get("/admin/credential/{credential_id}/capability-report")
def get_capability_report(
    credential_id: int,
    connector_id: int | None = None,
    user: User = Depends(
        require_permission(Permission.MANAGE_CONNECTORS, allow_scope=True)
    ),
    db_session: Session = Depends(get_session),
) -> CapabilityReportSnapshot | None:
    """Returns the stored report row for one scope, or None before any run.

    ``connector_id`` selects the connector-scoped row; without it the
    config-less credential-scoped row is returned.
    """
    # GATE 2 for ``allow_scope``: the user-filtered fetch is the visibility
    # check, and an unknown credential is indistinguishable from an
    # inaccessible one.
    credential = fetch_credential_by_id_for_user(credential_id, user, db_session)
    if credential is None:
        raise OnyxError(
            OnyxErrorCode.CREDENTIAL_NOT_FOUND,
            f"Credential {credential_id} does not exist or is not accessible.",
        )
    row = get_capability_report_row(db_session, credential_id, connector_id)
    return CapabilityReportSnapshot.from_row(row) if row is not None else None


@router.get("/admin/credential/capability-reports")
def list_capability_reports_for_source(
    source: DocumentSource,
    user: User = Depends(
        require_permission(Permission.MANAGE_CONNECTORS, allow_scope=True)
    ),
    db_session: Session = Depends(get_session),
) -> list[CapabilityReportSnapshot]:
    """Returns the report rows for a source's visible credentials, newest first."""
    # GATE 2 for ``allow_scope``: only rows of credentials the caller can see,
    # via the same user-filtered fetch the credential listings use.
    visible_credential_ids = {
        credential.id
        for credential in fetch_credentials_by_source_for_user(
            db_session=db_session,
            user=user,
            document_source=source,
        )
    }
    return [
        CapabilityReportSnapshot.from_row(row)
        for row in get_capability_report_rows_for_source(db_session, source)
        if row.credential_id in visible_credential_ids
    ]
