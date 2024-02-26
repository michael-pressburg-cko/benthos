package aws

import "sync"

func (k *kinesisReader) runEfoConsumer(wg *sync.WaitGroup, info streamInfo, shardID, startingSequence string) (initErr error) {
	//register consumer
	// busy read from consumer
	// copy code from runConsumer and use a channel to pass to pending
	return nil
}
