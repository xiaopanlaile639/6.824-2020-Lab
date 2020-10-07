package shardkv
//
//import (
//	"../shardmaster"
//	"time"
//)
//
//
//type PullArg struct {
//
//	PullShardId	int
//	NewConfig shardmaster.Config
//
//}
//
//type PullReply struct {
//
//	PullData	map[string]string			//pull回来 的数据
//	Err Err
//}
//
//
//func (kv*ShardKV) PullShard(newConfig* shardmaster.Config)Result{
//
//	retRes:=Result{
//		Err:    ErrWrongLeader,
//		RetVal: "",
//	}
//
//	if _,isLeader:=kv.rf.GetState();isLeader{		//leader
//		//rpc传送实际的数据
//		//chaPair:=kv.GetChaPair(newConfig)
//		chaPair:=kv.GetPullShardPair(newConfig)
//		chaDB,tmpRetRes:=kv.PullData(chaPair,*newConfig)
//
//		if tmpRetRes.Err != OK{			//如果拉取数据不成功的话，则直接返回失败
//			retRes.Err = tmpRetRes.Err
//			return retRes
//		}
//		retRes=kv.SendChaDBToRaft(chaDB,newConfig)
//
//	}
//
//	//kv.SetCurConfig(&newConfig)
//	//kv.SetChaFlag(false)	//处理完成
//
//	return retRes
//}
//
////把config发送给一个group的其他成员
//func (kv*ShardKV) SendChaDBToRaft(chaDB map[string] string,newConfig* shardmaster.Config)Result{
//
//	op:=Op{
//		Method:     SendShard,		//sendShard命令
//		MethodArgs: nil,
//		ClientId:   -1,
//		CmdIndex:   -1,
//		NewConfig: *newConfig,	//把Config也一起传送
//		ChaDB: chaDB,			//插入需要交换的DB
//
//	}
//
//	retRes:=Result{
//		Err:    ErrWrongLeader,
//		RetVal: "",
//	}
//
//	retRes=kv.SendCmdToRaft(-1,-1,op)		//发送执行命令
//
//	return retRes
//}
//
//
//
////向其他的group拉取数据
//func (kv*ShardKV) PullData(chaPair [] ChaShardPair,newConfig shardmaster.Config) (map[string]string,Result){
//
//	retRes:=Result{
//		Err:    OK,
//		RetVal: "",
//	}
//	tmpCurConfg:=kv.GetCurConfig()
//
//	retPullDB :=make(map[string]string)
//
//	for _,sinChaPair := range chaPair{			//每一对数据都需要交换
//
//		if servers, ok := tmpCurConfg.Groups[sinChaPair.fromGid]; ok {
//
//			pullArgs:=PullArg{
//				PullShardId: sinChaPair.shard,
//				NewConfig: newConfig,			//config也一起发送出去
//			}
//			pullReply:=PullReply{
//				PullData: nil,
//				Err:      NotOK,
//			}
//
//			//遍历toGid组的所有成员,从其leader组中获取数据
//			isPullOk:=false
//			for _,srv:=range servers{
//				srvPort := kv.make_end(srv)
//
//				ok:=srvPort.Call("ShardKV.PulledData", &pullArgs,&pullReply)
//				if ok && pullReply.Err == OK{			//从leader中获取到了数据
//					//添加pullData
//					for key,val:=range pullReply.PullData{
//						retPullDB[key] = val
//					}
//					isPullOk = true
//					break
//				}else{
//					//....
//				}
//			}
//
//				if isPullOk == false{
//					retRes.Err = ErrPullData
//
//					DPrintf("%v(%v) pull data failed from %v\n",kv.me,kv.gid,sinChaPair.fromGid)
//					return nil,retRes
//
//				}
//		}else{
//			DPrintf("%v(%v) curConfig is(%v),it lose config and should not be leader\n",kv.me,kv.gid,tmpCurConfg)
//		}
//	}
//
//	return retPullDB,retRes
//
//}
//
////
////func CopyToNewDBMap(src map[string]string,tar map[string]string)map[string]string{
////
////
////}
//
////rpc返回数据
//func (kv*ShardKV)PulledData(arg *PullArg,reply* PullReply){
//
//
//	kv.mu.Lock()
//	defer kv.mu.Unlock()
//
//	if kv.isChanging {			//如果正在交换数据，则立即返回
//		reply.Err = ErrBusy
//		return
//	}
//
//	//不是leader直接返回
//	if _,isLeader :=kv.rf.GetState(); isLeader == false{
//		reply.Err = ErrWrongLeader
//		return
//	}
//
//	DPrintf("%v(%v) is pulled data\n",kv.me,kv.gid)
//
//	retDB:=make(map[string]string)
//	for key,val:=range kv.DataBase{
//
//		if key2shard(key) == arg.PullShardId{
//			retDB[key] = val
//		}
//	}
//
//	reply.PullData = retDB
//	reply.Err = OK
//
//	//更新从别的组传来的newConfig(这样只针对于每次只有一个组有改变的情况)
//	//if kv.curConfig.Num < arg.NewConfig.Num{
//	//	//kv.SetCurConfig(&arg.NewConfig)
//	//	kv.curConfig = arg.NewConfig
//	//}
//
//	//kv.mu.Unlock()
//	kv.isChanging = true
//	//kv.SetChaFlag(true)			//准备接受数据，不再接受服务
//	///???update config...
//}
//
//func (kv*ShardKV) GetPullShardPair(newConfig* shardmaster.Config)[]ChaShardPair{
//
//	var newResShard []int
//	var chaPair []ChaShardPair
//
//	//获取当前组新的负责的shardId
//	for shardId,resGid := range newConfig.Shards{
//		if resGid == kv.gid{
//			newResShard = append(newResShard,shardId)
//		}
//	}
//
//	tmpCurConfig:=kv.GetCurConfig()
//
//	for _,shardId:=range newResShard{
//
//		tmpFromGid:=  tmpCurConfig.Shards[shardId]
//		tmpToGid:= kv.gid
//
//		if tmpFromGid != tmpToGid {
//
//			tmpChaPair:=ChaShardPair{
//				fromGid: tmpFromGid,			//采用的pull方式
//				toGid:   tmpToGid,
//				shard:   shardId,
//			}
//			chaPair = append(chaPair,tmpChaPair)
//		}
//	}
//
//	return chaPair
//}
//
////获取标志
//func (kv*ShardKV) GetChaFlag()bool{
//
//	kv.mu.Lock()
//	isCha:=kv.isChanging
//	kv.mu.Unlock()
//	return isCha
//}
//
//func (kv*ShardKV) GetCurConfig()shardmaster.Config{
//
//	kv.mu.Lock()
//	tmpCurCon:=kv.curConfig
//	kv.mu.Unlock()
//	return tmpCurCon
//
//}
//
////设置当前的config
//func (kv*ShardKV) SetCurConfig(curConfig* shardmaster.Config){
//	kv.mu.Lock()
//	kv.curConfig = *curConfig
//	kv.mu.Unlock()
//}
//
//
////获得最后一个config
//func (kv*ShardKV) FetLasCon(){
//
//	for {
//
//		tmpCurCfg := kv.GetCurConfig()
//		DPrintf("%v（%v）'s curConfig is %v\n",kv.me,kv.gid,tmpCurCfg)
//
//		if _,isLeader:=kv.rf.GetState(); isLeader{			//只有每个group的leader才进行询问操作
//
//			//newConfig := kv.mck.Query(-1)
//			nextConfigNum:=tmpCurCfg.Num+1
//			nextConfig:=kv.mck.Query(nextConfigNum)		//下一个config
//
//			if nextConfig.Num == nextConfigNum{		//如果确实是下一条config
//
//				DPrintf("%v（%v）rec nextConfig:%v\n",kv.me,kv.gid,nextConfig)
//
//				kv.SetChaFlag(true)  //正在交换数据
//
//				for kv.GetChaFlag() == true{		//一直处于交换数据状态，除非交换数据完成
//					res:=kv.PullShard(&nextConfig)
//
//					//kv.SetChaFlag(false)	//处理完成
//
//					if res.Err == OK{
//						kv.SetCurConfig(&nextConfig)
//						kv.SetChaFlag(false)	//处理完成
//						break
//					}else{
//
//						DPrintf("%v(%v) can't get data from other group, retry ...\n",kv.me,kv.gid)
//					}
//				}
//			}
//
//		}
//
//
//		time.Sleep(80*time.Millisecond)
//	}
//
//}
//
//////设置标志
////func (kv*ShardKV) SetChaFlag(isChaing bool){
////
////	kv.mu.Lock()
////	kv.isChanging = isChaing		//打上标志
////	kv.mu.Unlock()
////}