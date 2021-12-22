package summer.Uitl

object EnvCache {
/*
用于存放共享数据，例如根目录地址
 */
  private val envCache:ThreadLocal[Object] = new ThreadLocal[Object]

  def put(data:Object):Unit={
    envCache.set(data)
  }
  def get()={
    envCache.get()
  }

  def clear():Unit={
    envCache.remove()
  }
}
