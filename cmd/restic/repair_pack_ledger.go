package main

import (
	"github.com/restic/restic/internal/ledger"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/restic"
)

// ledgerQuarantineStore adapts the verification ledger to the repository
// package's repair executor interface.
type ledgerQuarantineStore struct {
	store *ledger.Store
}

var _ repository.QuarantineStore = &ledgerQuarantineStore{}

func toRepoStage(s ledger.Stage) repository.QuarantineStage {
	switch s {
	case ledger.StageRegistered:
		return repository.QuarantineRegistered
	case ledger.StageSalvaged:
		return repository.QuarantineSalvaged
	case ledger.StageRewritten:
		return repository.QuarantineRewritten
	default:
		return 0
	}
}

func (l *ledgerQuarantineStore) QuarantineRecords() []repository.QuarantineEntry {
	recs := l.store.Quarantine()
	out := make([]repository.QuarantineEntry, 0, len(recs))
	for _, rec := range recs {
		out = append(out, repository.QuarantineEntry{
			PackID:      rec.PackID,
			Stage:       toRepoStage(rec.Stage),
			Salvageable: rec.Salvageable,
		})
	}
	return out
}

func (l *ledgerQuarantineStore) MarkSalvaged(id restic.ID) error {
	return l.store.AdvanceStage(id, ledger.StageSalvaged)
}

func (l *ledgerQuarantineStore) MarkRewritten(ids restic.IDSet) error {
	return l.store.AdvanceStages(ids, ledger.StageRewritten)
}

func (l *ledgerQuarantineStore) Remove(id restic.ID) error {
	return l.store.AdvanceStage(id, ledger.StageRemoved)
}
