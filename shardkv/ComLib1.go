package shardkv

import (
	"../shardmaster"
	"time"
)

//把config发送给一个group的其他成员
func (kv*ShardKV) SendConToFol(curConfig *shardmaster.Config)Result{

	op:=Op{
		Method:     SendConfig,
		MethodArgs: nil,
		ClientId:   -1,
		CmdIndex:   -1,
		NewConfig: kv.GetCurConfig(),
		//NewConfig: *curConfig,
	}

	retRes:=Result{
		Err:    ErrWrongLeader,
		RetVal: "",
	}

	if _,isLeader:=kv.rf.GetState(); isLeader{		//只有是leader才能发送命令
		retRes=kv.SendCmdToRaft(-1,-1,op)		//发送执行命令
	}

	return retRes

}


////获取并删除当前DB中的shardId的K-V数据对
//func (kv*ShardKV) GetChaDB(shardId int)map[string]string{
//
//	kv.mu.Lock()
//	defer kv.mu.Unlock()
//
//	chaDB:=make(map[string]string)
//
//	for key,val:=range kv.DataBase{
//		if key2shard(key) == shardId{
//
//			chaDB[key] = val		//获取要传输的key-val键值对
//			delete(kv.DataBase,key)			//删除
//
//		}
//	}
//	return chaDB
//}
//
////接收到副本组中（leader）发过来的数据
////用于rpc通信
//func (kv*ShardKV)RecChaData(chaArgs *ChaArgs,chaReply *ChaReply){
//
//	////......
//	kv.mu.Lock()
//	defer kv.mu.Unlock()
//	kv.isChanging = true
//
//	DPrintf("%v in func RecChaData\n",kv.me)
//
//	for key,val:=range chaArgs.ChaDB{
//
//		if _,ok:=kv.DataBase[key];ok{
//			DPrintf("error:%v has %v-%v already\n",kv.me,key,val)
//		}else{
//			kv.DataBase[key] = val				//添加进当前的DB中
//		}
//	}
//
//	chaReply.Err = OK
//
//	//....
//}
//
////实际的数据交换
//func (kv*ShardKV) RealChaData (chaPair [] ChaShardPair,lastConfig *shardmaster.Config){
//
//
//	for _,sinChaPair := range chaPair{			//每一对数据都需要交换
//
//		if servers, ok := lastConfig.Groups[sinChaPair.toGid]; ok {
//
//			chaArgs:=ChaArgs{ChaDB: kv.GetChaDB(sinChaPair.shard)}
//			chaReply:=ChaReply{Err: OK}
//
//			//遍历toGid组的所有成员,把数据发送给他们
//			for _,srv:=range servers{
//				srvPort := kv.make_end(srv)
//
//				ok:=srvPort.Call("ShardKV.RecChaData", &chaArgs,&chaReply)
//
//				//....
//				if ok && chaReply.Err == OK{
//					//....
//				}else{
//					//....
//				}
//			}
//		}else{
//			DPrintf("something wrong with lastConfig's group\n")
//		}
//	}
//
//}
//
////删除DB中的数据
////适用于group中的非leader结点
//func (kv *ShardKV)DelDBData(chaPair [] ChaShardPair){
//
//	for _,sinChaPair := range chaPair { //每一对数据都需要交换
//
//		kv.GetChaDB(sinChaPair.shard)		//返回值不需要使用
//	}
//}


////获取需要交换的Pair
//func (kv*ShardKV) GetChaPair(lastConfig* shardmaster.Config)[]ChaShardPair{
//	var oldResShard []int
//	var chaPair []ChaShardPair
//
//	kv.mu.Lock()
//	//收集在新的config中，当前grooup 负责的shard
//	for shardId,resGid := range kv.curConfig.Shards{
//		if resGid == kv.gid{
//			oldResShard = append(oldResShard,shardId)
//		}
//	}
//	kv.mu.Unlock()
//
//	//构造传输对(以push的方式传输)
//	for _,shardId := range oldResShard {
//
//		tmpFromGid:=kv.gid
//		tmpToGid:= lastConfig.Shards[shardId]
//
//		if tmpFromGid != tmpToGid {
//
//			tmpChaPair:=ChaShardPair{
//				fromGid: kv.gid,			//采用的push的方式，所以from的gid就是kv的
//				toGid:   tmpToGid,
//				shard:   shardId,
//			}
//			chaPair = append(chaPair,tmpChaPair)
//			//chaShard = append(chaShard,shardId)
//		}
//	}
//	return chaPair
//}

////修改shards
//func (kv*ShardKV) ChangeShards(lastConfig* shardmaster.Config){
//
//	kv.SetChaFlag(true)
//	chaPair:=kv.GetChaPair(lastConfig)
//
//	if _,isLeader:=kv.rf.GetState();isLeader{		//leader
//		//rpc传送实际的数据
//		kv.RealChaData(chaPair,lastConfig)
//	}else{						//非leader
//		kv.DelDBData(chaPair)			//只需要删除数据
//	}
//
//	kv.SetCurConfig(lastConfig)
//	kv.SetChaFlag(false)
//}



//获得最后一个config
func (kv*ShardKV) FetLasCon(){

	for {

		DPrintf("%v（%v）'s curConfig is %v\n",kv.me,kv.gid,kv.GetCurConfig())

		//if kv.GetChaFlag() == true{		//如果正在交换数据
			if _,isLeader:=kv.rf.GetState(); isLeader{			//只有每个group的leader才进行询问操作

				newConfig := kv.mck.Query(-1)

				tmpCurConfig:=kv.GetCurConfig()
				if newConfig.Num > tmpCurConfig.Num{ //configuration已经发生了改变

					DPrintf("%v（%v）rec newConfig:%v\n",kv.me,kv.gid,newConfig)

					kv.SetChaFlag(true)  //正在交换数据

					res:=kv.PullShard(&newConfig)

					// kv.PullShard(&newConfig)

					kv.SetChaFlag(false)	//处理完成

					if res.Err == OK{

						kv.SetCurConfig(&newConfig)

					}else{
						//???...
					}
				}
				/*else if newConfig.Num == tmpCurConfig.Num{		//只发送config给follower
					kv.SendConToFol(&newConfig)
				}*/

				//kv.mu.Unlock()
				kv.SetChaFlag(false)	//处理完成
			}
		//}

		time.Sleep(80*time.Millisecond)
	}

}

//设置标志
func (kv*ShardKV) SetChaFlag(isChaing bool){

	kv.mu.Lock()
	kv.isChanging = isChaing		//打上标志
	kv.mu.Unlock()
}

//获取标志
func (kv*ShardKV) GetChaFlag()bool{

	kv.mu.Lock()
	isCha:=kv.isChanging
	kv.mu.Unlock()
	return isCha
}

func (kv*ShardKV) GetCurConfig()shardmaster.Config{

	kv.mu.Lock()
	tmpCurCon:=kv.curConfig
	kv.mu.Unlock()
	return tmpCurCon

}

//设置当前的config
func (kv*ShardKV) SetCurConfig(curConfig* shardmaster.Config){
	kv.mu.Lock()
	kv.curConfig = *curConfig
	kv.mu.Unlock()
}
