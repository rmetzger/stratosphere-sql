package eu.stratosphere.sql.relOpt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.util.Pair;

import eu.stratosphere.types.Value;

public class StratosphereRexUtils {

	public static class GetInputRefVisitor extends RexVisitorImpl<Void> {
		private List<Pair<Integer, RelDataType>> inputPosAndType = new ArrayList<Pair<Integer, RelDataType>>();

		public GetInputRefVisitor() {
			super(true);

		}

		public Void visitInputRef(RexInputRef inputRef) {
//			if (true) { // do I need a condition here?
//				inputRef.setExternalRef(true);
//			}
//			System.err.println("Setting input ref " + inputRef + " index "
//					+ inputRef.getIndex());
			inputPosAndType
					.add(Pair.of(inputRef.getIndex(), inputRef.getType()));
			return null;
		}

		public void resetInputList() {
			inputPosAndType.clear();
		}

		public List<Pair<Integer, RelDataType>> getInputPosAndType() {
			return inputPosAndType;
		}
	}

	public static class ProjectionFieldProperties implements Serializable {
		private static final long serialVersionUID = 1L;
		public Class<? extends Value> inFieldType;
		public Class<? extends Value> outFieldType;
		public int positionInInput;
		public int positionInOutput;
		public int positionInRex;
		public int fieldIndex;
		public boolean trivialProjection; // projection that does not need to execute generated code.
		public String name;


		@Override
		public String toString() {
			return "[ProjectionFieldProperties: inFieldType="+inFieldType+", "
					+ "outFieldType="+outFieldType+", "
					+ "positionInInput="+positionInInput+", "
					+ "positionInOutput="+positionInOutput+", "
					+ "positionInRex="+positionInRex+", "
					+ "fieldIndex="+fieldIndex+", "
					+ "trivialProjection="+trivialProjection+", "
					+ "name="+name+"]";
		}
		@Override
		public int hashCode() {
			HashCodeBuilder hash = new HashCodeBuilder();
			hash.append(positionInInput);
			hash.append(positionInOutput);
			hash.append(positionInRex);
			hash.append(fieldIndex);
			hash.append(trivialProjection);
			return hash.toHashCode();
		}


		public boolean equals(Object obj) {
			if(obj instanceof ProjectionFieldProperties) {
				ProjectionFieldProperties other = (ProjectionFieldProperties) obj;
				return other.positionInInput == positionInInput &&
						other.positionInOutput == positionInOutput &&
						other.positionInRex == positionInRex &&
						other.fieldIndex == fieldIndex &&
						other.trivialProjection == trivialProjection;
			}
			return false;
		}
	}

}
