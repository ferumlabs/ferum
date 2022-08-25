import Link from "next/link";

export enum BorderRadiusType {
  SMALL = "rounded-sm",
  MEDIUM = "rounded-[0.625rem]",
  LARGE = "rounded-full",
}

const Button = ({
  title,
  href,
  target,
  disabled,
  fullWidth = false,
  fullHeight = false,
  borderRadius = BorderRadiusType.LARGE,
  didTapButton,
}: {
  title: string;
  href?: string;
  target?: string;
  disabled?: boolean;
  fullWidth?: boolean,
  fullHeight?: boolean,
  borderRadius?: BorderRadiusType,
  didTapButton?: () => void,
}) => {
  const buttonStyle = `
   ${fullWidth ? "w-full" : ""} ${fullHeight ? "h-full" : ""} ${borderRadius} px-8 py-2 text-xl inline-flex items-center justify-center`;
  function ButtonAnchor() {
    return (
      <a
        className={`text-center inline-block cursor-pointer bg-gradient-to-r from-indigo-500 via-purple-500 to-pink-500 hover:from-indigo-400 hover:via-purple-400 hover:to-pink-400 text-white focus:ring transform transition hover:scale-105 duration-300 ease-in-out ${buttonStyle}`}
        onClick={didTapButton}
        target={target}
      >
        {title}
      </a>
    );
  }
  if (disabled) {
    return (
      <a
        className={`inline-block cursor-not-allowed bg-white bg-opacity-30 opacity-30 ${buttonStyle}`}
      >
        {title}
      </a>
    )
  }
  return (
    <>
      {href !== undefined ? (
        <Link href={href} passHref={true}>
          {ButtonAnchor()}
        </Link>
      ) : (
        <ButtonAnchor />
      )}
    </>
  );
};

export default Button;
