package eu.stratosphere.sql.schema;

// is a RuntimeException since I can not hand through the expections through Optiq.
public class SchemaAdapterException extends RuntimeException {
	public SchemaAdapterException(String string) {
		super(string);
	}

	public SchemaAdapterException(String string, ClassNotFoundException e) {
		super(string, e);
	}

	private static final long serialVersionUID = 1L;

}
