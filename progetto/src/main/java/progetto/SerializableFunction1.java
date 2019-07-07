package progetto;

import java.io.Serializable;

import scala.runtime.AbstractFunction1;

public abstract class SerializableFunction1<T,R> 
      extends AbstractFunction1<T, R> implements Serializable 
{
}