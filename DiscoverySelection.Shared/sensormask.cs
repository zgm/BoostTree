using System;
using System.Collections.Generic;
using System.Text;

namespace DiscoverySelection.Shared
{
    public class SensorMask : SerializableObject
    {
        internal UInt64 MaskValue = 0UL;

        public SensorMask()
        {

        }

        public SensorMask(UInt64 val)
        {
            MaskValue = val;
        }

        public SensorMask(SensorMask previousSensorMask)
        {
            MaskValue = previousSensorMask.MaskValue;
        }

        public SensorMask(SensorTypes sensorType)
        {
            MaskValue |= 1UL << (UInt16)sensorType;
        }

        public SensorMask(SensorTypes sensorType1, SensorTypes sensorType2)
        {
            MaskValue |= (1UL << (UInt16)sensorType1) | (1UL << (UInt16)sensorType2);

        }

        public SensorMask(SensorTypes sensorType1, SensorTypes sensorType2, SensorTypes sensorType3)
        {
            MaskValue |= (1UL << (UInt16)sensorType1) | (1UL << (UInt16)sensorType2) | (1UL << (UInt16)sensorType3);
        }

        public SensorMask(SensorTypes[] sensorTypes)
        {
            foreach (SensorTypes sensorType in sensorTypes)
            {
                MaskValue |= 1UL << (UInt16)sensorType;
            }
        }

        public SensorMask(SensorMask previousSensorMask, SensorTypes sensorType)
        {
            MaskValue = previousSensorMask.MaskValue;
            MaskValue |= 1UL << (UInt16)sensorType;
        }

        public SensorMask(SensorMask previousSensorMask, SensorTypes[] sensorTypes)
        {
            MaskValue = previousSensorMask.MaskValue;
            foreach (SensorTypes sensorType in sensorTypes)
            {
                MaskValue |= 1UL << (UInt16)sensorType;
            }
        }

        public void CopySensorMaskFrom(SensorMask sensorMask)
        {
            this.MaskValue = sensorMask.MaskValue;
        }

        public void AddSensorType(SensorTypes sensorType)
        {
            this.MaskValue = this.MaskValue | 1UL << (UInt16)sensorType;
        }

        public void RemoveSensorType(SensorTypes sensorType)
        {
            this.MaskValue = this.MaskValue & ~(1UL << (UInt16)sensorType);
        }

        public void SetSensorType(SensorTypes sensorType)
        {
            this.MaskValue = 1UL << (UInt16)sensorType;
        }

        public Boolean HasASensor(SensorTypes sensorType)
        {
            return (MaskValue & (1UL << (UInt16)sensorType)) != 0UL;
        }


        public Boolean HasCommonSensor(SensorMask sensorMask)
        {
            return (this.MaskValue & sensorMask.MaskValue) != 0UL;
        }

        public static SensorMask operator~ (SensorMask sensorMask)
        {
            SensorMask newSensoMask = new SensorMask();
            newSensoMask.MaskValue = ~sensorMask.MaskValue;

            return newSensoMask;
        }

        public static SensorMask operator| (SensorMask sensorMask1, SensorMask sensorMask2)
        {
            SensorMask newSensorMask = new SensorMask();
            newSensorMask.MaskValue = sensorMask1.MaskValue | sensorMask2.MaskValue;

            return newSensorMask;
        }

        //public override String Object.String ToString()
        public String ToDebugString()
        {
            // this can probably be optimized

            string ret = "";
            int numTypes = Enum.GetNames(typeof(SensorTypes)).Length;

            for (int i = 0; i < numTypes; i++)
            {
                if (((1UL << i) & (UInt64)MaskValue) > 0)
                {
                    if (ret.Length > 0)
                    {
                        ret += "|" + ((SensorTypes)i).ToString();
                    }
                    else
                    {
                        ret = ((SensorTypes)i).ToString();
                    }
                }
            }

            return ret;

        }

        public UInt64 Long
        {
            get { return MaskValue; }
        }

        public Boolean IsEmpty
        {
            get { return MaskValue == 0UL; }
        }

        public void CombineWith(SensorMask sensorMask)
        {
            MaskValue |= sensorMask.MaskValue;
        }

        public override void Serialize(System.IO.BinaryWriter bw)
        {
            bw.Write(MaskValue);
        }

        public override void Deserialize(System.IO.BinaryReader br)
        {
            MaskValue = br.ReadUInt64();
        }

        public override bool Equals(object obj)
        {
            if (obj == null)
            {
                return false;
            }

            SensorMask sensorMask = obj as SensorMask;

            if ((object)sensorMask == null)
            {
                return false;
            }

            return this.MaskValue == sensorMask.MaskValue;
        }

        public bool Equals(SensorMask sensorMask)
        {
            // If parameter is null return false:
            if ((object)sensorMask == null)
            {
                return false;
            }

            // Return true if the fields match:
            return this.MaskValue == sensorMask.MaskValue;
        }

        public static Boolean operator==(SensorMask sensorMask1, SensorMask sensorMask2)
        {
            if ((object)sensorMask1 == null)
            {
                return (object)sensorMask2 == null;
            }
            else
            {
                return sensorMask1.Equals(sensorMask2);
            }
        }

        public static Boolean operator !=(SensorMask sensorMask1, SensorMask sensorMask2)
        {
            return !(sensorMask1 == sensorMask2);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }


    }
}
