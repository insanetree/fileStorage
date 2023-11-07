import multiprocessing
import threading
import yaml
import hashlib
import zlib
import os

def processCommand(manager, cmd, path, chunkSize, pool, semaphore, fileRegistryDictionary, fileRegistryMutex, chunkRegistryDictionary, chunkRegistryMutex):
    if cmd[0] == "put":
        put(manager, cmd[1:], path, chunkSize, pool, semaphore, fileRegistryDictionary, fileRegistryMutex, chunkRegistryDictionary, chunkRegistryMutex)
    elif cmd[0] == "list":
        list(fileRegistryDictionary, fileRegistryMutex)
    elif cmd[0] == "get":
        get(manager, path, int(cmd[1]), cmd[2], pool, semaphore, fileRegistryDictionary, fileRegistryMutex, chunkRegistryDictionary, chunkRegistryMutex)
    elif cmd[0] == "delete":
        delete(map(int, cmd[1:]), path, pool, semaphore, fileRegistryDictionary, fileRegistryMutex, chunkRegistryDictionary, chunkRegistryMutex)

######################################################
"""
format:
put (fileName)+
"""
def put(manager, arg, path, chunkSize, pool, semaphore, fileRegistryDictionary, fileRegistryMutex, chunkRegistryDictionary, chunkRegistryMutex):
    for fileName in arg:
        if os.path.isfile(fileName):

            with fileRegistryMutex:
                fileID = fileRegistryDictionary["nextID"]
                newInput = manager.list([fileName, "put"])
                fileRegistryDictionary[fileID] = newInput
                fileRegistryDictionary["nextID"]+=1
            

            with open(fileName, "rb") as fileIn:
                results = []
                fileChunkInfo = manager.list()
                while fileIn.tell() != os.path.getsize(fileName):
                    semaphore.acquire() # Memory management semaphore

                    chunk = fileIn.read(chunkSize)

                    with chunkRegistryMutex:
                        chunkID = chunkRegistryDictionary["nextID"]
                        chunkRegistryDictionary["nextID"] += 1
                    
                    results.append(pool.apply_async(putPoolProcess, kwds={"path": path,
                                                                          "chunkID": chunkID,
                                                                          "chunk": chunk,
                                                                          "semaphore":semaphore}))
                for r in results:
                    fileChunkInfo.append(r.get())
                with chunkRegistryMutex:
                    chunkRegistryDictionary[fileID] = fileChunkInfo
                with fileRegistryMutex:
                    fileRegistryDictionary[fileID][1] = "ready"

def putPoolProcess(path, chunkID, chunk, semaphore):
    md5Hash = hashlib.md5(chunk).hexdigest()
    compressed = zlib.compress(chunk)
    with open("{}/{}.dat".format(path, chunkID), "wb") as chunkOut:
        chunkOut.write(compressed)
    semaphore.release()
    return (chunkID, md5Hash)
######################################################
"""
format:
get fileID savePath
"""
def get(manager, basePath, fileID, savePath, pool, semaphore, fileRegistryDictionary, fileRegistryMutex, chunkRegistryDictionary, chunkRegistryMutex):
    if not os.path.isdir(savePath):
        print(savePath, "does not exist")
        return
    with fileRegistryMutex:
        if fileID not in fileRegistryDictionary.keys() or fileRegistryDictionary[fileID][1] != "ready":
            print("File with provided fileID does not exist or is not ready")
            return
        fileName = fileRegistryDictionary[fileID][0]
        fileRegistryDictionary[fileID][1] = "get"
        with chunkRegistryMutex:
            chunks = chunkRegistryDictionary[fileID]
    
    
    results = []
    for chunk in chunks:
        chunkID = chunk[0]
        chunkHash = chunk[1]
        results.append(pool.apply_async(getPoolProcess, kwds={"basePath": basePath,
                                                              "chunkID": chunkID,
                                                              "chunkHash": chunkHash,
                                                              "semaphore": semaphore}))
    
    with open("{}/{}".format(savePath, fileName), "wb") as fileOut:
        flagOk = True
        for x in results:
            if x.ready() and not x.successful():
                flagOk = False
            r = x.get() if flagOk else None
            if flagOk and r[0]:
                fileOut.write(r[1])
            if flagOk and not r[0]:
                flagOk = False
            semaphore.release()
    with fileRegistryMutex:
        fileRegistryDictionary[fileID][1] = "ready" if flagOk else "corrupted"

def getPoolProcess(basePath, chunkID, chunkHash, semaphore):
    semaphore.acquire()
    with open("{}/{}.dat".format(basePath, chunkID), "rb") as cIn:
        block = zlib.decompress(cIn.read())
    md5Hash = hashlib.md5(block).hexdigest()
    if chunkHash != md5Hash:
        return (False, md5Hash)
    return (True, block)
    
######################################################    

def list(fileRegistryDictionary, fileRegistryMutex):
    with fileRegistryMutex:
        print("\nfileID filename status")
        for key in fileRegistryDictionary.keys():
            if key == "nextID":
                continue
            print(key, fileRegistryDictionary[key][0], fileRegistryDictionary[key][1])
######################################################

def delete(files, basePath, pool, semaphore, fileRegistryDictionary, fileRegistryMutex, chunkRegistryDictionary, chunkRegistryMutex):
    for fileID in files:
        with fileRegistryMutex:
            if fileID not in fileRegistryDictionary.keys() or fileRegistryDictionary[fileID][1] != "ready":
                print("File with provided fileID ({}) does not exist or is not ready".format(fileID))
                continue
            fileRegistryDictionary[fileID][1] = "delete"
            with chunkRegistryMutex:
                chunks = chunkRegistryDictionary[fileID]
        results = []
        for chunk in chunks:
            chunkID = chunk[0]
            results.append(pool.apply_async(deletePoolProcess, kwds={"basePath":basePath,
                                                                     "chunkID":chunkID}))
        for result in results:
            deleted = result.get()
            if deleted != False:
                with chunkRegistryMutex:
                    for x in chunkRegistryDictionary[fileID]:
                        if x[0] == deleted:
                            chunkRegistryDictionary[fileID].remove(x)
                            break
        with chunkRegistryMutex:
            chunkRegistryDictionary.pop(fileID)
        with fileRegistryMutex:
            fileRegistryDictionary.pop(fileID)
            
def deletePoolProcess(basePath, chunkID):
    fileName = "{}/{}.dat".format(basePath, chunkID)
    if(os.path.exists(fileName)):
        os.remove(fileName)
        return chunkID
    return False
    

if __name__ == '__main__':
    """
    System init
    Reading configuration
    Opening process Pool
    Opening Semaphore for memory usage
    """
    mainConfig = yaml.safe_load(open("config.yaml", "r"))
    savePath = mainConfig["savePath"]
    IOProcessNum = mainConfig["IOProcessNum"]
    maxMem = mainConfig["maxMem"]
    chunkSize = mainConfig["chunkSize"]

    processPool = multiprocessing.Pool(IOProcessNum)
    manager = multiprocessing.Manager()
    availableMemorySemaphore = manager.Semaphore(maxMem // chunkSize)
    fileRegistryDictionary = manager.dict()
    fileRegistryMutex = manager.Lock()
    chunkRegistryDictionary = manager.dict()
    chunkRegistryMutex = manager.Lock()

    fileRegistryDictionary["nextID"] = 1
    chunkRegistryDictionary["nextID"] = 1
    
    while(True):
        cmd = input("system> ")
        if cmd == "":
            continue
        cmd = cmd.split()
        if cmd[0] == "exit":
            print('system shutdown')
            processPool.close()
            processPool.join()

            print(fileRegistryDictionary)
            print(chunkRegistryDictionary)

            break
        if cmd[0] in ["put", "get", "list", "delete"]:
            t = threading.Thread(target=processCommand, kwargs={"manager": manager,
                                                                "cmd": cmd,
                                                                "path": savePath,
                                                                "pool": processPool,
                                                                "chunkSize": chunkSize,
                                                                "semaphore": availableMemorySemaphore,
                                                                "fileRegistryDictionary": fileRegistryDictionary,
                                                                "fileRegistryMutex": fileRegistryMutex,
                                                                "chunkRegistryDictionary": chunkRegistryDictionary,
                                                                "chunkRegistryMutex": chunkRegistryMutex
                                                                })
            t.start()
            continue
        print("Command does not exist")