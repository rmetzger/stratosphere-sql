package eu.stratosphere.sql.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;

import net.hydromatic.avatica.ColumnMetaData;
import net.hydromatic.avatica.Cursor;

public class StratosphereCursor implements Cursor {
	private StratosphereResultSet resultSet;
	public StratosphereCursor(StratosphereResultSet resultSet) {
		this.resultSet = resultSet;
	}

	@Override
	public List<Accessor> createAccessors(List<ColumnMetaData> types,
			Calendar localCalendar) {
		System.err.println("Types "+types.size());
		return ImmutableList.of(new StraAccessor(), (Accessor)new StraAccessor());
	}

	static boolean i = false;
	@Override
	public boolean next() throws SQLException {
		if(!i) {
			i = true;
			return true;
		}
		return false;
	}

	@Override
	public void close() {
		throw new RuntimeException("implMe");
	}

	@Override
	public boolean wasNull() throws SQLException {
		return false;
	}
	
	public static class StraAccessor implements Accessor {

		@Override
		public boolean wasNull() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public String getString() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public boolean getBoolean() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public byte getByte() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public short getShort() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public int getInt() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public long getLong() throws SQLException {
			return 3;
		}

		@Override
		public float getFloat() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public double getDouble() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public BigDecimal getBigDecimal() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public BigDecimal getBigDecimal(int scale) throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public byte[] getBytes() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public InputStream getAsciiStream() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public InputStream getUnicodeStream() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public InputStream getBinaryStream() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public Object getObject() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public Reader getCharacterStream() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public Object getObject(Map<String, Class<?>> map) throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public Ref getRef() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public Blob getBlob() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public Clob getClob() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public Array getArray() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public Date getDate(Calendar calendar) throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public Time getTime(Calendar calendar) throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public Timestamp getTimestamp(Calendar calendar) throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public URL getURL() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public NClob getNClob() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public SQLXML getSQLXML() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public String getNString() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public Reader getNCharacterStream() throws SQLException {
			throw new RuntimeException("implMe");
		}

		@Override
		public <T> T getObject(Class<T> type) throws SQLException {
			throw new RuntimeException("implMe");
		}
		
	}

}
