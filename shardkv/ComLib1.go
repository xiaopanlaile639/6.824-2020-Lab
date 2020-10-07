package shardkv

import (
	"../shardmaster"
	"sync"
	"time"
)

type MigrateArgs struct {
	Shard     int
	ConfigNum int
}

type MigrateReply struct {
	Err         Err
	ConfigNum   int
	Shard       int
	DB          map[string]string
	Cid2Seq     map[int64]int
}


func (kv *ShardKV) TryPollNewCfg() {

	for {

		if _, isLeader := kv.rf.GetState(); isLeader {

			if kv.IsNeedChangShard() == false{			//如果不存在还未处理的shards
				tmpCurCfg := kv.GetCurConfig()

				nextConfigNum:=tmpCurCfg.Num+1
				nextConfig:=kv.mck.Query(nextConfigNum)		//下一个config

				if nextConfig.Num == nextConfigNum{		//如果确实是下一条config
					kv.rf.Start(nextConfig)				//向底层raft系统发送Config
				}
			}
		}
		time.Sleep(50*time.Millisecond)
	}
}


func (kv *ShardKV) TryPullShard() {

	for {
		if _,isLeader:=kv.rf.GetState();isLeader{
			if kv.IsNeedChangShard() == true{		//需要处理shard

				var wait sync.WaitGroup
				for shard, idx := range kv.comeInShards {		//shard->configIndex
					wait.Add(1)

					//处理每个shard
					go func(shard int, cfg shardmaster.Config) {
						defer wait.Done()
						args := MigrateArgs{shard, cfg.Num}
						gid := cfg.Shards[shard]
						for _, server := range cfg.Groups[gid] {
							srv := kv.make_end(server)
							reply := MigrateReply{}
							ok := srv.Call("ShardKV.ShardMigration", &args, &reply)
							if  ok && reply.Err == OK {
								kv.rf.Start(reply)				//向底层raft发送数据等信息
							}
						}
					}(shard, kv.mck.Query(idx))
				}

				wait.Wait()
			}
		}

		time.Sleep(50*time.Millisecond)
	}
}

func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply *MigrateReply) {
	reply.Err, reply.Shard, reply.ConfigNum = ErrWrongLeader, args.Shard, args.ConfigNum
	if _,isLeader := kv.rf.GetState(); !isLeader {return}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup

	//只处理比当前config小的情况
	//比当前config大的，需要当前config稍后更新前移
	if args.ConfigNum >= kv.curConfig.Num {
		return
	}

	reply.Err,reply.ConfigNum, reply.Shard = OK, args.ConfigNum, args.Shard
	reply.DB, reply.Cid2Seq = kv.deepCopyDBAndDedupMap(args.ConfigNum,args.Shard)
}

func (kv *ShardKV) deepCopyDBAndDedupMap(config int,shard int) (map[string]string, map[int64]int) {
	db2 := make(map[string]string)
	cid2Seq2 := make(map[int64]int)

	for k, v := range kv.toOutShards[config][shard] {
		db2[k] = v
	}

	//???
	for k, v := range kv.lastCmdIndexMap {
		cid2Seq2[k] = v
	}
	return db2, cid2Seq2
}



func (kv *ShardKV) UpdateDBWithMigrateData(migrationData MigrateReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if migrationData.ConfigNum != kv.curConfig.Num-1 {
		return
	}

	delete(kv.comeInShards, migrationData.Shard)		//在需要补充的shard中删除准备处理的shard

	//this check is necessary, to avoid use  kv.cfg.Num-1 to update kv.cfg.Num's shard
	if _, ok := kv.myShards[migrationData.Shard]; !ok {
		kv.myShards[migrationData.Shard] = true

		//混合数据部分
		for k, v := range migrationData.DB {
			kv.DataBase[k] = v
		}

		for k, v := range migrationData.Cid2Seq {
			kv.lastCmdIndexMap[k] = Max(v,kv.lastCmdIndexMap[k])
		}

		//if _, ok := kv.garbages[migrationData.ConfigNum]; !ok {
		//	kv.garbages[migrationData.ConfigNum] = make(map[int]bool)
		//}
		//kv.garbages[migrationData.ConfigNum][migrationData.Shard] = true
	}
}


func (kv *ShardKV) UpdateInAndOutDataShard(cfg shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if cfg.Num <= kv.curConfig.Num { //only consider newer config
		return
	}
	oldCfg, toOutShard := kv.curConfig, kv.myShards
	kv.myShards, kv.curConfig = make(map[int]bool), cfg

	for shard, gid := range cfg.Shards {
		if gid != kv.gid {
			continue
		}

		if _, ok := toOutShard[shard]; ok || oldCfg.Num == 0 {
			kv.myShards[shard] = true		//oldConfig中也具有的shard
			delete(toOutShard, shard)		//删除属于自己的shard
		} else {
			kv.comeInShards[shard] = oldCfg.Num		//如果没有的话，记录oldConfig.num
		}
	}
	if len(toOutShard) > 0 { // prepare data that needed migration
		kv.toOutShards[oldCfg.Num] = make(map[int]map[string]string)
		for shard := range toOutShard {
			outDb := make(map[string]string)
			for k, v := range kv.DataBase {
				if key2shard(k) == shard {
					outDb[k] = v
					delete(kv.DataBase, k)
				}
			}
			kv.toOutShards[oldCfg.Num][shard] = outDb
		}
	}
}




//是否需要交换shard
func (kv*ShardKV) IsNeedChangShard()bool{
	kv.mu.Lock()
	numOfComInShard:=len(kv.comeInShards)
	kv.mu.Unlock()

	if numOfComInShard > 0{
		return true
	}else{
		return false
	}
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func (kv*ShardKV) GetCurConfig()shardmaster.Config{

	kv.mu.Lock()
	tmpCurCon:=kv.curConfig
	kv.mu.Unlock()
	return tmpCurCon

}